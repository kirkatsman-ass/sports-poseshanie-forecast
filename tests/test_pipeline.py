import csv
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

spec=importlib.util.spec_from_file_location('pipeline',Path(__file__).resolve().parents[1]/'src/pipeline.py')
p=importlib.util.module_from_spec(spec);spec.loader.exec_module(p)

class PipelineTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory(); self.old=p.ROOT;p.ROOT=Path(self.temp.name)
        self.cfg={'seed':29,'start_year':2021,'years':5,'capacity':20000,'horizon_days':7,
                  'sources_directory':'raw','database':'db/sports.db'}
        p.generate(self.cfg)
    def tearDown(self): p.ROOT=self.old;self.temp.cleanup()
    def test_reproducible_and_consistent(self):
        file=p.ROOT/'raw/matches.csv'; before=file.read_bytes();p.generate(self.cfg)
        self.assertEqual(before,file.read_bytes())
        with file.open() as f: matches=list(csv.DictReader(f))
        with (p.ROOT/'raw/ticket_sales.csv').open() as f: tickets=list(csv.DictReader(f))
        self.assertEqual(len(matches),120);self.assertEqual(len(tickets),3600)
        self.assertEqual({x['match_id'] for x in matches},{x['match_id'] for x in tickets})
        for match in matches:
            total=sum(int(x['net_tickets']) for x in tickets if x['match_id']==match['match_id'])
            self.assertEqual(total,int(match['final_tickets']))
            self.assertLessEqual(int(match['attendance']),total)
    def test_repeat_and_increment(self):
        first=p.ingest(self.cfg);self.assertEqual(sum(x['rows_inserted'] for x in first),3840)
        self.assertEqual(sum(x['rows_inserted'] for x in p.ingest(self.cfg)),0)
        f=p.ROOT/'raw/weather_forecasts.json';rows=json.loads(f.read_text())
        row=rows[0].copy();row['forecast_id']+='-new-version';rows.append(row)
        f.write_text(json.dumps(rows))
        self.assertEqual(sum(x['rows_inserted'] for x in p.ingest(self.cfg)),1)
    def test_conflict_rolls_back_and_logs(self):
        p.ingest(self.cfg);f=p.ROOT/'raw/weather_forecasts.json';rows=json.loads(f.read_text())
        new=rows[0].copy();new['forecast_id']='NEW';rows.insert(0,new)
        rows[1]['temperature_2m']=999;f.write_text(json.dumps(rows))
        with self.assertRaises(ValueError):p.ingest(self.cfg)
        db=p.connect(self.cfg)
        self.assertEqual(db.execute('SELECT count(*) FROM records').fetchone()[0],3840)
        self.assertEqual(db.execute('SELECT status FROM load_log ORDER BY run_id DESC LIMIT 1').fetchone()[0],'failed')
        db.close()
    def test_missing_source_is_logged(self):
        (p.ROOT/'raw/matches.csv').unlink()
        with self.assertRaises(FileNotFoundError):p.ingest(self.cfg)
        db=p.connect(self.cfg)
        self.assertEqual(db.execute('SELECT status FROM load_log').fetchone()[0],'failed')
        db.close()

if __name__=='__main__':unittest.main()
