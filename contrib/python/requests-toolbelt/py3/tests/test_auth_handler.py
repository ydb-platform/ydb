import requests
from requests.auth import HTTPBasicAuth
from requests_toolbelt.auth.handler import AuthHandler
from requests_toolbelt.auth.handler import NullAuthStrategy


def test_turns_tuples_into_basic_auth():
    a = AuthHandler({'http://example.com': ('foo', 'bar')})
    strategy = a.get_strategy_for('http://example.com')
    assert not isinstance(strategy, NullAuthStrategy)
    assert isinstance(strategy, HTTPBasicAuth)


def test_uses_null_strategy_for_non_matching_domains():
    a = AuthHandler({'http://api.example.com': ('foo', 'bar')})
    strategy = a.get_strategy_for('http://example.com')
    assert isinstance(strategy, NullAuthStrategy)


def test_normalizes_domain_keys():
    a = AuthHandler({'https://API.github.COM': ('foo', 'bar')})
    assert 'https://api.github.com' in a.strategies
    assert 'https://API.github.COM' not in a.strategies


def test_can_add_new_strategies():
    a = AuthHandler({'https://example.com': ('foo', 'bar')})
    a.add_strategy('https://api.github.com', ('fiz', 'baz'))
    assert isinstance(
        a.get_strategy_for('https://api.github.com'),
        HTTPBasicAuth
        )


def test_prepares_auth_correctly():
    # Set up our Session and AuthHandler
    auth = AuthHandler({
        'https://api.example.com': ('bar', 'baz'),
        'https://httpbin.org': ('biz', 'fiz'),
    })
    s = requests.Session()
    s.auth = auth
    # Set up a valid GET request to https://api.example.com/users
    r1 = requests.Request('GET', 'https://api.example.com/users')
    p1 = s.prepare_request(r1)
    assert p1.headers['Authorization'] == 'Basic YmFyOmJheg=='

    # Set up a valid POST request to https://httpbin.org/post
    r2 = requests.Request('POST', 'https://httpbin.org/post', data='foo')
    p2 = s.prepare_request(r2)
    assert p2.headers['Authorization'] == 'Basic Yml6OmZpeg=='

    # Set up an *invalid* OPTIONS request to http://api.example.com
    # NOTE(sigmavirus24): This is not because of the verb but instead because
    # it is the wrong URI scheme.
    r3 = requests.Request('OPTIONS', 'http://api.example.com/projects')
    p3 = s.prepare_request(r3)
    assert p3.headers.get('Authorization') is None
