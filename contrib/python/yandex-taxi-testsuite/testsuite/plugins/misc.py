import importlib

# Ignore tracebacks from these modules
_tracebackhide_modules = [
    'contextlib',
    'concurrent.futures._base',
    'concurrent.futures.thread',
]


def pytest_sessionstart():
    for mod in _get_tracebackhide_modules():
        setattr(mod, '__tracebackhide__', True)


def pytest_sessionfinish():
    for mod in _get_tracebackhide_modules():
        delattr(mod, '__tracebackhide__')


def _get_tracebackhide_modules():
    for modname in _tracebackhide_modules:
        try:
            mod = importlib.import_module(modname)
        except ImportError:
            continue
        yield mod
