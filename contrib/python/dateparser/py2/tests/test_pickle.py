import pickle
from dateparser import parse
from dateparser.timezone_parser import StaticTzInfo
from datetime import datetime
from tests import BaseTestCase


class TestPickle(BaseTestCase):
    def test_pickling_basic_parsed_object(self):
        now = parse('now')
        self.assertIsInstance(now, datetime)
        pickle_dumps = pickle.dumps(now)
        self.assertIsInstance(pickle_dumps, bytes)

    def test_pickling_parsed_object_with_tz(self):
        now_in_utc = parse('now in UTC')
        self.assertIsInstance(now_in_utc, datetime)
        self.assertIsInstance(now_in_utc.tzinfo, StaticTzInfo)
        pickle_dumps = pickle.dumps(now_in_utc)
        self.assertIsInstance(pickle_dumps, bytes)

    def test_unpickling_basic_parsed_object(self):
        now = parse('now')
        pickle_dumps = pickle.dumps(now)
        pickle_loads = pickle.loads(pickle_dumps)
        self.assertIsInstance(pickle_loads, datetime)

    def test_unpickling_parsed_object_with_tz(self):
        now_in_utc = parse('now in UTC')
        pickle_dumps = pickle.dumps(now_in_utc)
        pickle_loads = pickle.loads(pickle_dumps)
        self.assertIsInstance(pickle_loads, datetime)
        self.assertIsInstance(pickle_loads.tzinfo, StaticTzInfo)
