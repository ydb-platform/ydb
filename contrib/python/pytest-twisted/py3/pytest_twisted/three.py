from twisted.internet import defer


@defer.inlineCallbacks
def _async_pytest_fixture_setup(fixturedef, request, mark):
    """Setup an async or async yield fixture."""
    from pytest_twisted import (
        UnrecognizedCoroutineMarkError,
        _create_async_yield_fixture_finalizer,
    )

    fixture_function = fixturedef.func

    kwargs = {
        name: request.getfixturevalue(name)
        for name in fixturedef.argnames
    }

    if mark == 'async_fixture':
        arg_value = yield defer.ensureDeferred(
            fixture_function(**kwargs)
        )
    elif mark == 'async_yield_fixture':
        coroutine = fixture_function(**kwargs)

        request.addfinalizer(
            _create_async_yield_fixture_finalizer(coroutine=coroutine),
        )

        arg_value = yield defer.ensureDeferred(coroutine.__anext__())
    else:
        raise UnrecognizedCoroutineMarkError.from_mark(mark=mark)

    fixturedef.cached_result = (arg_value, fixturedef.cache_key(request), None)

    return arg_value


@defer.inlineCallbacks
def _async_pytest_pyfunc_call(pyfuncitem, f, kwargs):
    """Run test function."""
    from pytest_twisted import _get_mark

    fixture_kwargs = {
        name: value
        for name, value in pyfuncitem.funcargs.items()
        if name in pyfuncitem._fixtureinfo.argnames
    }
    kwargs.update(fixture_kwargs)

    maybe_mark = _get_mark(f)
    if maybe_mark == 'async_test':
        result = yield defer.ensureDeferred(f(**kwargs))
    elif maybe_mark == 'inline_callbacks_test':
        result = yield f(**kwargs)
    else:
        # TODO: maybe deprecate this
        result = yield f(**kwargs)

    return result
