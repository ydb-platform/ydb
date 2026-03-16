import pytest

from testsuite import matching


def _default_regex_match(doc: dict):
    return matching.RegexString(doc['pattern'])


def _default_partial_dict_match(doc: dict):
    return matching.PartialDict(doc['value'])


def _match_unordered_list(doc: dict):
    items = doc['items']
    if 'keys' in doc:
        key = _make_keys_getter(doc['keys'])
    elif 'key' in doc:
        key = _make_key_getter(doc['key'])
    else:
        key = None
    return matching.unordered_list(items, key=key)


def _match_list_of(doc):
    return matching.ListOf(value=doc.get('item', matching.any_value))


def _match_dict_of(doc):
    return matching.DictOf(
        key=doc.get('key', matching.any_value),
        value=doc.get('value', matching.any_value),
    )


def pytest_register_matching_hooks():
    return {
        'any-value': matching.any_value,
        'any-float': matching.any_float,
        'any-integer': matching.any_integer,
        'any-numeric': matching.any_numeric,
        'positive-float': matching.positive_float,
        'positive-integer': matching.positive_integer,
        'positive-numeric': matching.positive_numeric,
        'negative-float': matching.negative_float,
        'negative-integer': matching.negative_integer,
        'negative-numeric': matching.negative_numeric,
        'non-negative-float': matching.non_negative_float,
        'non-negative-integer': matching.non_negative_integer,
        'non-negative-numeric': matching.non_negative_numeric,
        'any-string': matching.any_string,
        'uuid-string': matching.uuid_string,
        'objectid-string': matching.objectid_string,
        'datetime-string': matching.datetime_string,
        'regex': _default_regex_match,
        # dictionaries
        'any-dict': matching.any_dict,
        'dict-of': _match_dict_of,
        'partial-dict': _default_partial_dict_match,
        # lists
        'any-list': matching.any_list,
        'list-of': _match_list_of,
        'unordered-list': _match_unordered_list,
        'unordered_list': _match_unordered_list,
    }


class Hookspec:
    def pytest_register_matching_hooks(self):
        pass


class MatchingPlugin:
    def __init__(self):
        self._matching_hooks = {}

    @property
    def matching_hooks(self):
        return self._matching_hooks

    def pytest_sessionstart(self, session):
        hooks = (
            session.config.pluginmanager.hook.pytest_register_matching_hooks()
        )
        for hook in hooks:
            self._matching_hooks.update(hook)

    def pytest_addhooks(self, pluginmanager):
        pluginmanager.add_hookspecs(Hookspec)


def pytest_configure(config):
    config.pluginmanager.register(MatchingPlugin(), 'matching_params')


@pytest.fixture(scope='session')
def operator_match(request, pytestconfig):
    plugin = pytestconfig.pluginmanager.get_plugin('matching_params')

    def match(doc: dict):
        match_type = doc.get('type')
        try:
            hook = plugin.matching_hooks[match_type]
        except KeyError:
            raise RuntimeError(f'Unknown match type {match_type}')
        if callable(hook):
            return hook(doc)
        return hook

    return match


@pytest.fixture(scope='session')
def match_operator(operator_match):
    def _wrapper(doc: dict):
        match = doc['$match']
        if isinstance(match, str):
            match = {'type': match}
        return operator_match(match)

    return _wrapper


def _make_keys_getter(keys):
    key_getters = tuple(_make_key_getter(key) for key in keys)

    def getter(value):
        return tuple(getter(value) for getter in key_getters)

    return getter


def _make_key_getter(path):
    if isinstance(path, str):
        path = [path]

    def getter(doc):
        for key in path:
            doc = doc[key]
        return doc

    return getter
