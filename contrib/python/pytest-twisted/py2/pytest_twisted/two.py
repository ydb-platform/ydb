from twisted.internet import defer


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

    defer.returnValue(result)
