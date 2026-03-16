from .cassette import Cassette
from .exceptions import InvalidOption, validation_error_map


def validate_record(record):
    return record in ['all', 'new_episodes', 'none', 'once']


def validate_matchers(matchers):
    from betamax.matchers import matcher_registry
    available_matchers = list(matcher_registry.keys())
    return all(m in available_matchers for m in matchers)


def validate_serializer(serializer):
    from betamax.serializers import serializer_registry
    return serializer in list(serializer_registry.keys())


def validate_placeholders(placeholders):
    """Validate placeholders is a dict-like structure"""
    keys = ['placeholder', 'replace']
    try:
        return all(sorted(list(p.keys())) == keys for p in placeholders)
    except TypeError:
        return False


def translate_cassette_options():
    for (k, v) in Cassette.default_cassette_options.items():
        yield (k, v) if k != 'record_mode' else ('record', v)


def isboolean(value):
    return value in [True, False]


class Options(object):
    valid_options = {
        'match_requests_on': validate_matchers,
        're_record_interval': lambda x: x is None or x > 0,
        'record': validate_record,
        'serialize': validate_serializer,  # TODO: Remove this
        'serialize_with': validate_serializer,
        'preserve_exact_body_bytes': isboolean,
        'placeholders': validate_placeholders,
        'allow_playback_repeats': isboolean,
    }

    defaults = {
        'match_requests_on': ['method', 'uri'],
        're_record_interval': None,
        'record': 'once',
        'serialize': None,  # TODO: Remove this
        'serialize_with': 'json',
        'preserve_exact_body_bytes': False,
        'placeholders': [],
        'allow_playback_repeats': False,
    }

    def __init__(self, data=None):
        self.data = data or {}
        self.validate()
        self.defaults = Options.defaults.copy()
        self.defaults.update(translate_cassette_options())

    def __repr__(self):
        return 'Options(%s)' % self.data

    def __getitem__(self, key):
        return self.data.get(key, self.defaults.get(key))

    def __setitem__(self, key, value):
        self.data[key] = value
        return value

    def __delitem__(self, key):
        del self.data[key]

    def __contains__(self, key):
        return key in self.data

    def items(self):
        return self.data.items()

    def validate(self):
        for key, value in list(self.data.items()):
            if key not in Options.valid_options:
                raise InvalidOption('{0} is not a valid option'.format(key))
            else:
                is_valid = Options.valid_options[key]
                if not is_valid(value):
                    raise validation_error_map[key]('{0!r} is not valid'
                                                    .format(value))
