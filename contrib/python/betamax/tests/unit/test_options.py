import unittest
from itertools import permutations

import pytest

from betamax import exceptions
from betamax.options import Options, validate_record, validate_matchers


class TestValidators(unittest.TestCase):
    def test_validate_record(self):
        for mode in ['once', 'none', 'all', 'new_episodes']:
            assert validate_record(mode) is True

    def test_validate_matchers(self):
        matchers = ['method', 'uri', 'query', 'host', 'body']
        for i in range(1, len(matchers)):
            for l in permutations(matchers, i):
                assert validate_matchers(l) is True

        matchers.append('foobar')
        assert validate_matchers(matchers) is False


class TestOptions(unittest.TestCase):
    def setUp(self):
        self.data = {
            're_record_interval': 10000,
            'match_requests_on': ['method'],
            'serialize': 'json'
        }
        self.options = Options(self.data)

    def test_data_is_valid(self):
        for key in self.data:
            assert key in self.options

    def test_raise_on_unknown_option(self):
        data = self.data.copy()
        data['fake'] = 'value'
        with pytest.raises(exceptions.InvalidOption):
            Options(data)

    def test_raise_on_invalid_body_bytes(self):
        data = self.data.copy()
        data['preserve_exact_body_bytes'] = None
        with pytest.raises(exceptions.BodyBytesValidationError):
            Options(data)

    def test_raise_on_invalid_matchers(self):
        data = self.data.copy()
        data['match_requests_on'] = ['foo', 'bar', 'bogus']
        with pytest.raises(exceptions.MatchersValidationError):
            Options(data)

    def test_raise_on_invalid_placeholders(self):
        data = self.data.copy()
        data['placeholders'] = None
        with pytest.raises(exceptions.PlaceholdersValidationError):
            Options(data)

    def test_raise_on_invalid_playback_repeats(self):
        data = self.data.copy()
        data['allow_playback_repeats'] = None
        with pytest.raises(exceptions.PlaybackRepeatsValidationError):
            Options(data)

    def test_raise_on_invalid_record(self):
        data = self.data.copy()
        data['record'] = None
        with pytest.raises(exceptions.RecordValidationError):
            Options(data)

    def test_raise_on_invalid_record_interval(self):
        data = self.data.copy()
        data['re_record_interval'] = -1
        with pytest.raises(exceptions.RecordIntervalValidationError):
            Options(data)

    def test_raise_on_invalid_serializer(self):
        data = self.data.copy()
        data['serialize_with'] = None
        with pytest.raises(exceptions.SerializerValidationError):
            Options(data)
