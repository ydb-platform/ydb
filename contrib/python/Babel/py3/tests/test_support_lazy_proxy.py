import copy

import pytest

from babel import support


def test_proxy_caches_result_of_function_call():
    counter = 0

    def add_one():
        nonlocal counter
        counter += 1
        return counter

    proxy = support.LazyProxy(add_one)
    assert proxy.value == 1
    assert proxy.value == 1


def test_can_disable_proxy_cache():
    counter = 0

    def add_one():
        nonlocal counter
        counter += 1
        return counter

    proxy = support.LazyProxy(add_one, enable_cache=False)
    assert proxy.value == 1
    assert proxy.value == 2


@pytest.mark.parametrize(("copier", "expected_copy_value"), [
    (copy.copy, 2),
    (copy.deepcopy, 1),
])
def test_can_copy_proxy(copier, expected_copy_value):
    numbers = [1, 2]

    def first(xs):
        return xs[0]

    proxy = support.LazyProxy(first, numbers)
    proxy_copy = copier(proxy)

    numbers.pop(0)
    assert proxy.value == 2
    assert proxy_copy.value == expected_copy_value


def test_handle_attribute_error():
    def raise_attribute_error():
        raise AttributeError('message')

    proxy = support.LazyProxy(raise_attribute_error)
    with pytest.raises(AttributeError, match='message'):
        _ = proxy.value


def test_lazy_proxy():
    def greeting(name='world'):
        return f"Hello, {name}!"

    lazy_greeting = support.LazyProxy(greeting, name='Joe')
    assert str(lazy_greeting) == "Hello, Joe!"
    assert '  ' + lazy_greeting == '  Hello, Joe!'
    assert '(%s)' % lazy_greeting == '(Hello, Joe!)'
    assert f"[{lazy_greeting}]" == "[Hello, Joe!]"

    greetings = sorted([
        support.LazyProxy(greeting, 'world'),
        support.LazyProxy(greeting, 'Joe'),
        support.LazyProxy(greeting, 'universe'),
    ])
    assert [str(g) for g in greetings] == [
        "Hello, Joe!",
        "Hello, universe!",
        "Hello, world!",
    ]
