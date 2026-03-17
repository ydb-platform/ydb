# -*- coding: utf-8 -*-
import inspect
import sys
import warnings

import pytest

import deprecated.classic


class MyDeprecationWarning(DeprecationWarning):
    pass


class WrongStackLevelWarning(DeprecationWarning):
    pass


_PARAMS = [
    None,
    ((), {}),
    (('Good reason',), {}),
    ((), {'reason': 'Good reason'}),
    ((), {'version': '1.2.3'}),
    ((), {'action': 'once'}),
    ((), {'category': MyDeprecationWarning}),
    ((), {'extra_stacklevel': 1, 'category': WrongStackLevelWarning}),
]


@pytest.fixture(scope="module", params=_PARAMS)
def classic_deprecated_function(request):
    if request.param is None:

        @deprecated.classic.deprecated
        def foo1():
            pass

        return foo1
    else:
        args, kwargs = request.param

        @deprecated.classic.deprecated(*args, **kwargs)
        def foo1():
            pass

        return foo1


@pytest.fixture(scope="module", params=_PARAMS)
def classic_deprecated_class(request):
    if request.param is None:

        @deprecated.classic.deprecated
        class Foo2(object):
            pass

        return Foo2
    else:
        args, kwargs = request.param

        @deprecated.classic.deprecated(*args, **kwargs)
        class Foo2(object):
            pass

        return Foo2


@pytest.fixture(scope="module", params=_PARAMS)
def classic_deprecated_method(request):
    if request.param is None:

        class Foo3(object):
            @deprecated.classic.deprecated
            def foo3(self):
                pass

        return Foo3
    else:
        args, kwargs = request.param

        class Foo3(object):
            @deprecated.classic.deprecated(*args, **kwargs)
            def foo3(self):
                pass

        return Foo3


@pytest.fixture(scope="module", params=_PARAMS)
def classic_deprecated_static_method(request):
    if request.param is None:

        class Foo4(object):
            @staticmethod
            @deprecated.classic.deprecated
            def foo4():
                pass

        return Foo4.foo4
    else:
        args, kwargs = request.param

        class Foo4(object):
            @staticmethod
            @deprecated.classic.deprecated(*args, **kwargs)
            def foo4():
                pass

        return Foo4.foo4


@pytest.fixture(scope="module", params=_PARAMS)
def classic_deprecated_class_method(request):
    if request.param is None:

        class Foo5(object):
            @classmethod
            @deprecated.classic.deprecated
            def foo5(cls):
                pass

        return Foo5
    else:
        args, kwargs = request.param

        class Foo5(object):
            @classmethod
            @deprecated.classic.deprecated(*args, **kwargs)
            def foo5(cls):
                pass

        return Foo5


# noinspection PyShadowingNames
def test_classic_deprecated_function__warns(classic_deprecated_function):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        classic_deprecated_function()
    assert len(warns) == 1
    warn = warns[0]
    assert issubclass(warn.category, DeprecationWarning)
    assert "deprecated function (or staticmethod)" in str(warn.message)


# noinspection PyShadowingNames
def test_classic_deprecated_class__warns(classic_deprecated_class):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        classic_deprecated_class()
    assert len(warns) == 1
    warn = warns[0]
    assert issubclass(warn.category, DeprecationWarning)
    assert "deprecated class" in str(warn.message)


# noinspection PyShadowingNames
def test_classic_deprecated_method__warns(classic_deprecated_method):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        obj = classic_deprecated_method()
        obj.foo3()
    assert len(warns) == 1
    warn = warns[0]
    assert issubclass(warn.category, DeprecationWarning)
    assert "deprecated method" in str(warn.message)


# noinspection PyShadowingNames
def test_classic_deprecated_static_method__warns(classic_deprecated_static_method):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        classic_deprecated_static_method()
    assert len(warns) == 1
    warn = warns[0]
    assert issubclass(warn.category, DeprecationWarning)
    assert "deprecated function (or staticmethod)" in str(warn.message)


# noinspection PyShadowingNames
def test_classic_deprecated_class_method__warns(classic_deprecated_class_method):
    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        cls = classic_deprecated_class_method()
        cls.foo5()
    assert len(warns) == 1
    warn = warns[0]
    assert issubclass(warn.category, DeprecationWarning)
    if (3, 9) <= sys.version_info < (3, 13):
        assert "deprecated class method" in str(warn.message)
    else:
        assert "deprecated function (or staticmethod)" in str(warn.message)


def test_should_raise_type_error():
    try:
        deprecated.classic.deprecated(5)
        assert False, "TypeError not raised"
    except TypeError:
        pass


def test_warning_msg_has_reason():
    reason = "Good reason"

    @deprecated.classic.deprecated(reason=reason)
    def foo():
        pass

    with warnings.catch_warnings(record=True) as warns:
        foo()
    warn = warns[0]
    assert reason in str(warn.message)


def test_warning_msg_has_version():
    version = "1.2.3"

    @deprecated.classic.deprecated(version=version)
    def foo():
        pass

    with warnings.catch_warnings(record=True) as warns:
        foo()
    warn = warns[0]
    assert version in str(warn.message)


def test_warning_is_ignored():
    @deprecated.classic.deprecated(action='ignore')
    def foo():
        pass

    with warnings.catch_warnings(record=True) as warns:
        foo()
    assert len(warns) == 0


def test_specific_warning_cls_is_used():
    @deprecated.classic.deprecated(category=MyDeprecationWarning)
    def foo():
        pass

    with warnings.catch_warnings(record=True) as warns:
        foo()
    warn = warns[0]
    assert issubclass(warn.category, MyDeprecationWarning)


def test_respect_global_filter():
    @deprecated.classic.deprecated(version='1.2.1', reason="deprecated function")
    def fun():
        print("fun")

    warnings.simplefilter("once", category=DeprecationWarning)

    with warnings.catch_warnings(record=True) as warns:
        fun()
        fun()
    assert len(warns) == 1


def test_default_stacklevel():
    """
    The objective of this unit test is to ensure that the triggered warning message,
    when invoking the 'use_foo' function, correctly indicates the line where the
    deprecated 'foo' function is called.
    """

    @deprecated.classic.deprecated
    def foo():
        pass

    def use_foo():
        foo()

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        use_foo()

    # Check that the warning path matches the module path
    warn = warns[0]
    assert warn.filename == __file__

    # Check that the line number points to the first line inside 'use_foo'
    use_foo_lineno = inspect.getsourcelines(use_foo)[1]
    assert warn.lineno == use_foo_lineno + 1


def test_extra_stacklevel():
    """
    The unit test utilizes an 'extra_stacklevel' of 1 to ensure that the warning message
    accurately identifies the caller of the deprecated function. It verifies that when
    the 'use_foo' function is called, the warning message correctly indicates the line
    where the call to 'use_foo' is made.
    """

    @deprecated.classic.deprecated(extra_stacklevel=1)
    def foo():
        pass

    def use_foo():
        foo()

    def demo():
        use_foo()

    with warnings.catch_warnings(record=True) as warns:
        warnings.simplefilter("always")
        demo()

    # Check that the warning path matches the module path
    warn = warns[0]
    assert warn.filename == __file__

    # Check that the line number points to the first line inside 'demo'
    demo_lineno = inspect.getsourcelines(demo)[1]
    assert warn.lineno == demo_lineno + 1
