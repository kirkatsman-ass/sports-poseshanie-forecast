"""Initial educational data pipeline. Every bundled observation is synthetic."""
import argparse
import csv
import hashlib
import json
import math
from pathlib import Path
import random
import sqlite3
import time
from datetime import datetime, timedelta, timezone
from urllib.error import HTTPError, URLError
from urllib.request import urlopen
from urllib.parse import urlencode

ROOT = Path(__file__).resolve().parents[1]

def config():
    return json.loads((ROOT / 'config/settings.json').read_text(encoding='utf-8'))

def write_csv(path, rows):
    with path.open('w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)

def generate(cfg):
    """Deterministic example, not measured sporting or meteorological data."""
    rng = random.Random(cfg['seed'])
    out = ROOT / cfg['sources_directory']
    out.mkdir(parents=True, exist_ok=True)
    matches, tickets, weather = [], [], []
    for year in range(cfg['start_year'], cfg['start_year'] + cfg['years']):
        for month in range(1, 13):
            for day in (10, 24):
                dt = datetime(year, month, day, 18, 0, tzinfo=timezone.utc)
                mid = f'M{year}{month:02}{day}'
                cutoff = dt - timedelta(days=cfg['horizon_days'])
                popularity = rng.randint(1, 5)
                price = rng.choice([700, 900, 1100, 1300])
                temp = 8 + 14 * math.sin(2 * math.pi * (month - 4) / 12) + rng.gauss(0, 2)
                rain = round(max(0, rng.gauss(2, 2)), 1)
                demand = 11000 + 1100 * popularity - 3 * price + 800 * (dt.weekday() >= 5) + 80 * temp - 160 * rain + rng.gauss(0, 800)
                total = int(max(1000, min(cfg['capacity'], demand)))
                attendance = int(total * rng.uniform(.88, .98))
                matches.append(dict(match_id=mid, kickoff=dt.isoformat(),
                    published_at=(dt-timedelta(days=35)).isoformat(),
                    result_available_at=(dt+timedelta(days=1)).isoformat(),
                    home_team='Учебный клуб', away_team=f'Соперник {popularity}',
                    stadium_id='S01', capacity=cfg['capacity'],
                    opponent_rating=popularity, announced_price_rub=price,
                    final_tickets=total, attendance=attendance, is_synthetic=1))
                weights = [rng.uniform(.5, 1.5)*(1+i/15) for i in range(30)]
                daily = [int(total*x/sum(weights)) for x in weights]
                daily[-1] += total - sum(daily)
                for i, net in enumerate(daily):
                    sale_day = (dt-timedelta(days=30-i)).date().isoformat()
                    returns = rng.randint(0, 4)
                    tickets.append(dict(event_id=f'{mid}-{sale_day}',match_id=mid,
                        sale_date=sale_day, available_at=sale_day+'T23:59:59+00:00',
                        sold=net+returns, refunded=returns, net_tickets=net,
                        gross_rub=(net+returns)*price, refund_rub=returns*price,
                        is_synthetic=1))
                weather.append(dict(forecast_id=f'{mid}-D7',match_id=mid,
                    stadium_id='S01',valid_at=dt.isoformat(),issued_at=cutoff.isoformat(),
                    temperature_2m=round(temp,1),precipitation=rain,
                    source='synthetic_weather_forecast',is_synthetic=1))
    write_csv(out/'matches.csv', matches)
    write_csv(out/'ticket_sales.csv', tickets)
    (out/'weather_forecasts.json').write_text(json.dumps(weather,ensure_ascii=False,indent=2),encoding='utf-8')
    return dict(matches=len(matches),tickets=len(tickets),weather=len(weather))

def connect(cfg):
    path = ROOT / cfg['database']
    path.parent.mkdir(parents=True,exist_ok=True)
    db=sqlite3.connect(path)
    db.executescript('''
      CREATE TABLE IF NOT EXISTS records(
        source TEXT NOT NULL, record_id TEXT NOT NULL, payload TEXT NOT NULL,
        PRIMARY KEY(source,record_id));
      CREATE TABLE IF NOT EXISTS source_state(
        source TEXT PRIMARY KEY, sha256 TEXT NOT NULL);
      CREATE TABLE IF NOT EXISTS load_log(
        run_id INTEGER PRIMARY KEY AUTOINCREMENT, started_at TEXT NOT NULL,
        source TEXT NOT NULL, status TEXT NOT NULL,
        rows_read INTEGER NOT NULL, rows_inserted INTEGER NOT NULL, error TEXT);
    ''')
    return db

def validate(source, rows):
    keys={'matches':'match_id','ticket_sales':'event_id','weather_forecasts':'forecast_id'}
    key=keys[source]
    seen={}
    for row in rows:
        rid=row[key]
        if not rid: raise ValueError('Empty record identifier')
        if rid in seen and row!=seen[rid]: raise ValueError('Conflicting duplicate: '+rid)
        seen[rid]=row
        if source=='matches':
            if not 0<=int(row['attendance'])<=int(row['final_tickets'])<=int(row['capacity']):
                raise ValueError('Invalid attendance or ticket capacity: '+rid)
            datetime.fromisoformat(row['kickoff'])
        elif source=='ticket_sales':
            if int(row['sold'])<0 or int(row['refunded'])<0: raise ValueError('Negative ticket count')
            if int(row['sold'])-int(row['refunded'])!=int(row['net_tickets']): raise ValueError('Invalid net count')
            datetime.fromisoformat(row['available_at'])
        else:
            if datetime.fromisoformat(row['issued_at'])>datetime.fromisoformat(row['valid_at']):
                raise ValueError('Forecast issued after event')
    return key

def ingest_file(db,path):
    started=datetime.now(timezone.utc).isoformat()
    source=path.stem
    n=added=0
    try:
        digest=hashlib.sha256(path.read_bytes()).hexdigest()
        old=db.execute('SELECT sha256 FROM source_state WHERE source=?',(source,)).fetchone()
        if old and old[0]==digest:
            status='unchanged'
        else:
            if path.suffix=='.csv':
                with path.open(encoding='utf-8',newline='') as f: rows=list(csv.DictReader(f))
            else: rows=json.loads(path.read_text(encoding='utf-8'))
            n=len(rows)
            key=validate(source,rows)
            with db:
                for row in rows:
                    payload=json.dumps(row,sort_keys=True,ensure_ascii=False)
                    prior=db.execute('SELECT payload FROM records WHERE source=? AND record_id=?',(source,row[key])).fetchone()
                    if prior and prior[0]!=payload:
                        raise ValueError('Existing identifier changed; explicit version required: '+row[key])
                    cur=db.execute('INSERT INTO records VALUES(?,?,?) ON CONFLICT DO NOTHING',(source,row[key],payload))
                    added+=cur.rowcount
                db.execute('INSERT INTO source_state VALUES(?,?) ON CONFLICT(source) DO UPDATE SET sha256=excluded.sha256',(source,digest))
            status='loaded'
        with db:
            db.execute('INSERT INTO load_log(started_at,source,status,rows_read,rows_inserted,error) VALUES(?,?,?,?,?,NULL)',(started,source,status,n,added))
        return dict(source=source,status=status,rows_read=n,rows_inserted=added)
    except (OSError,ValueError,KeyError,TypeError,sqlite3.Error) as exc:
        db.rollback()
        with db:
            db.execute('INSERT INTO load_log(started_at,source,status,rows_read,rows_inserted,error) VALUES(?,?,?,?,?,?)',(started,source,'failed',n,0,str(exc)))
        raise

def ingest(cfg):
    db=connect(cfg)
    try:
        return [ingest_file(db,ROOT/cfg['sources_directory']/f) for f in
                ['matches.csv','ticket_sales.csv','weather_forecasts.json']]
    finally: db.close()

def fetch_weather(cfg):
    """Live independent source. Never substitutes for historical D7 forecasts."""
    params=dict(latitude=cfg['weather_latitude'],longitude=cfg['weather_longitude'],
                hourly='temperature_2m,precipitation',timezone='UTC',forecast_days=8)
    url='https://api.open-meteo.com/v1/forecast?'+urlencode(params)
    last=None
    for attempt in range(cfg['http_attempts']):
        try:
            with urlopen(url,timeout=cfg['http_timeout_seconds']) as response:
                data=json.load(response)
            if not isinstance(data.get('hourly'),dict): raise ValueError('Missing hourly fields')
            received=datetime.now(timezone.utc)
            snapshot=dict(source_url=url,retrieved_at=received.isoformat(),is_synthetic=False,response=data)
            out=ROOT/cfg['sources_directory']/'live_weather'
            out.mkdir(parents=True,exist_ok=True)
            file=out/(received.strftime('%Y%m%dT%H%M%S%f')+'.json')
            file.write_text(json.dumps(snapshot,ensure_ascii=False,indent=2),encoding='utf-8')
            return dict(saved=str(file.relative_to(ROOT)),status='live_snapshot_saved')
        except HTTPError as exc:
            last=exc
            if exc.code!=429 and exc.code<500: raise
        except (URLError,TimeoutError) as exc: last=exc
        if attempt<cfg['http_attempts']-1: time.sleep(2**attempt)
    raise RuntimeError('Weather source unavailable after retries') from last

def main():
    p=argparse.ArgumentParser()
    p.add_argument('command',choices=['generate','ingest','demo','fetch-weather'])
    args=p.parse_args(); cfg=config()
    if args.command=='generate': result=generate(cfg)
    elif args.command=='ingest': result=ingest(cfg)
    elif args.command=='fetch-weather': result=fetch_weather(cfg)
    else:
        result={'generated':generate(cfg),'ingested':ingest(cfg)}
    print(json.dumps(result,ensure_ascii=False,indent=2))

if __name__=='__main__':main()
