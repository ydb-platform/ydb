import sys

import pytest

PYPY = '__pypy__' in sys.builtin_module_names


@pytest.fixture(scope="session")
def lop_loader():
    def load_implementation(name):
        class FakeModule:
            subclass = False
            kind = name
            if name == "slots":
                from lazy_object_proxy.slots import Proxy
            elif name == "simple":
                from lazy_object_proxy.simple import Proxy
            elif name == "cext":
                try:
                    from lazy_object_proxy.cext import Proxy
                except ImportError:
                    if PYPY:
                        pytest.skip(msg="C Extension not available.")
                    else:
                        raise
            elif name == "objproxies":
                Proxy = pytest.importorskip("objproxies").LazyProxy
            elif name == "django":
                Proxy = pytest.importorskip("django.utils.functional").SimpleLazyObject
            else:
                raise RuntimeError("Unsupported param: %r." % name)

            Proxy

        return FakeModule

    return load_implementation


@pytest.fixture(scope="session", params=[
    "slots", "cext",
    "simple",
    # "external-django", "external-objproxies"
])
def lop_implementation(request, lop_loader):
    return lop_loader(request.param)


@pytest.fixture(scope="session", params=[True, False], ids=['subclassed', 'normal'])
def lop_subclass(request, lop_implementation):
    if request.param:
        class submod(lop_implementation):
            subclass = True
            Proxy = type("SubclassOf_" + lop_implementation.Proxy.__name__,
                         (lop_implementation.Proxy,), {})

        return submod
    else:
        return lop_implementation


@pytest.fixture(scope="function")
def lop(request, lop_subclass):
    if request.node.get_closest_marker('xfail_subclass'):
        request.applymarker(pytest.mark.xfail(
            reason="This test can't work because subclassing disables certain "
                   "features like __doc__ and __module__ proxying."
        ))
    if request.node.get_closest_marker('xfail_simple'):
        request.applymarker(pytest.mark.xfail(
            reason="The lazy_object_proxy.simple.Proxy has some limitations."
        ))

    return lop_subclass
