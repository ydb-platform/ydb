import os
import platform
import sys
from contextlib import contextmanager

import py.code
import pytest

pytest_plugins = "pytester"

# could not make some of the tests work on PyPy, patches are welcome!
skip_pypy = pytest.mark.skipif(
    platform.python_implementation() == "PyPy", reason="could not make it work on pypy"
)

# Python 3.8 changed the output formatting (bpo-35500), which has been ported to mock 3.0
NEW_FORMATTING = sys.version_info >= (3, 8) or sys.version_info[0] == 2


@pytest.fixture
def needs_assert_rewrite(pytestconfig):
    """
    Fixture which skips requesting test if assertion rewrite is disabled (#102)

    Making this a fixture to avoid acessing pytest's config in the global context.
    """
    option = pytestconfig.getoption("assertmode")
    if option != "rewrite":
        pytest.skip(
            "this test needs assertion rewrite to work but current option "
            'is "{}"'.format(option)
        )


class UnixFS(object):
    """
    Wrapper to os functions to simulate a Unix file system, used for testing
    the mock fixture.
    """

    @classmethod
    def rm(cls, filename):
        os.remove(filename)

    @classmethod
    def ls(cls, path):
        return os.listdir(path)


@pytest.fixture
def check_unix_fs_mocked(tmpdir, mocker):
    """
    performs a standard test in a UnixFS, assuming that both `os.remove` and
    `os.listdir` have been mocked previously.
    """

    def check(mocked_rm, mocked_ls):
        assert mocked_rm is os.remove
        assert mocked_ls is os.listdir

        file_name = tmpdir / "foo.txt"
        file_name.ensure()

        UnixFS.rm(str(file_name))
        mocked_rm.assert_called_once_with(str(file_name))
        assert os.path.isfile(str(file_name))

        mocked_ls.return_value = ["bar.txt"]
        assert UnixFS.ls(str(tmpdir)) == ["bar.txt"]
        mocked_ls.assert_called_once_with(str(tmpdir))

        mocker.stopall()

        assert UnixFS.ls(str(tmpdir)) == ["foo.txt"]
        UnixFS.rm(str(file_name))
        assert not os.path.isfile(str(file_name))

    return check


def mock_using_patch_object(mocker):
    return mocker.patch.object(os, "remove"), mocker.patch.object(os, "listdir")


def mock_using_patch(mocker):
    return mocker.patch("os.remove"), mocker.patch("os.listdir")


def mock_using_patch_multiple(mocker):
    r = mocker.patch.multiple("os", remove=mocker.DEFAULT, listdir=mocker.DEFAULT)
    return r["remove"], r["listdir"]


@pytest.mark.parametrize(
    "mock_fs", [mock_using_patch_object, mock_using_patch, mock_using_patch_multiple]
)
def test_mock_patches(mock_fs, mocker, check_unix_fs_mocked):
    """
    Installs mocks into `os` functions and performs a standard testing of
    mock functionality. We parametrize different mock methods to ensure
    all (intended, at least) mock API is covered.
    """
    # mock it twice on purpose to ensure we unmock it correctly later
    mock_fs(mocker)
    mocked_rm, mocked_ls = mock_fs(mocker)
    check_unix_fs_mocked(mocked_rm, mocked_ls)
    mocker.resetall()
    mocker.stopall()


def test_mock_patch_dict(mocker):
    """
    Testing
    :param mock:
    """
    x = {"original": 1}
    mocker.patch.dict(x, values=[("new", 10)], clear=True)
    assert x == {"new": 10}
    mocker.stopall()
    assert x == {"original": 1}


def test_mock_patch_dict_resetall(mocker):
    """
    We can call resetall after patching a dict.
    :param mock:
    """
    x = {"original": 1}
    mocker.patch.dict(x, values=[("new", 10)], clear=True)
    assert x == {"new": 10}
    mocker.resetall()
    assert x == {"new": 10}


@pytest.mark.parametrize(
    "name",
    [
        "ANY",
        "call",
        "create_autospec",
        "MagicMock",
        "Mock",
        "mock_open",
        "NonCallableMock",
        "PropertyMock",
        "sentinel",
    ],
)
def test_mocker_aliases(name, pytestconfig):
    from pytest_mock import _get_mock_module, MockFixture

    mock_module = _get_mock_module(pytestconfig)

    mocker = MockFixture(pytestconfig)
    assert getattr(mocker, name) is getattr(mock_module, name)


def test_mocker_resetall(mocker):
    listdir = mocker.patch("os.listdir")
    open = mocker.patch("os.open")

    listdir("/tmp")
    open("/tmp/foo.txt")
    listdir.assert_called_once_with("/tmp")
    open.assert_called_once_with("/tmp/foo.txt")

    mocker.resetall()

    assert not listdir.called
    assert not open.called


class TestMockerStub:
    def test_call(self, mocker):
        stub = mocker.stub()
        stub("foo", "bar")
        stub.assert_called_once_with("foo", "bar")

    def test_repr_with_no_name(self, mocker):
        stub = mocker.stub()
        assert "name" not in repr(stub)

    def test_repr_with_name(self, mocker):
        test_name = "funny walk"
        stub = mocker.stub(name=test_name)
        assert "name={0!r}".format(test_name) in repr(stub)

    def __test_failure_message(self, mocker, **kwargs):
        expected_name = kwargs.get("name") or "mock"
        if NEW_FORMATTING:
            msg = "expected call not found.\nExpected: {0}()\nActual: not called."
        else:
            msg = "Expected call: {0}()\nNot called"
        expected_message = msg.format(expected_name)
        stub = mocker.stub(**kwargs)
        with pytest.raises(AssertionError) as exc_info:
            stub.assert_called_with()
        assert str(exc_info.value) == expected_message

    def test_failure_message_with_no_name(self, mocker):
        self.__test_failure_message(mocker)

    @pytest.mark.parametrize("name", (None, "", "f", "The Castle of aaarrrrggh"))
    def test_failure_message_with_name(self, mocker, name):
        self.__test_failure_message(mocker, name=name)


def test_instance_method_spy(mocker):
    class Foo(object):
        def bar(self, arg):
            return arg * 2

    foo = Foo()
    other = Foo()
    spy = mocker.spy(foo, "bar")
    assert foo.bar(arg=10) == 20
    assert other.bar(arg=10) == 20
    foo.bar.assert_called_once_with(arg=10)
    assert foo.bar.spy_return == 20
    spy.assert_called_once_with(arg=10)
    assert spy.spy_return == 20


def test_instance_method_spy_exception(mocker):
    class Foo(object):
        def bar(self, arg):
            raise Exception("Error with {}".format(arg))

    foo = Foo()
    spy = mocker.spy(foo, "bar")

    expected_calls = []
    for i, v in enumerate([10, 20]):
        with pytest.raises(Exception, match="Error with {}".format(v)) as exc_info:
            foo.bar(arg=v)

        expected_calls.append(mocker.call(arg=v))
        assert foo.bar.call_args_list == expected_calls
        assert str(spy.spy_exception) == "Error with {}".format(v)


def test_spy_reset(mocker):
    class Foo(object):
        def bar(self, x):
            if x == 0:
                raise ValueError("invalid x")
            return x * 3

    spy = mocker.spy(Foo, "bar")
    assert spy.spy_return is None
    assert spy.spy_exception is None

    Foo().bar(10)
    assert spy.spy_return == 30
    assert spy.spy_exception is None

    with pytest.raises(ValueError):
        Foo().bar(0)
    assert spy.spy_return is None
    assert str(spy.spy_exception) == "invalid x"

    Foo().bar(15)
    assert spy.spy_return == 45
    assert spy.spy_exception is None


@skip_pypy
def test_instance_method_by_class_spy(mocker):
    class Foo(object):
        def bar(self, arg):
            return arg * 2

    spy = mocker.spy(Foo, "bar")
    foo = Foo()
    other = Foo()
    assert foo.bar(arg=10) == 20
    assert other.bar(arg=10) == 20
    calls = [mocker.call(foo, arg=10), mocker.call(other, arg=10)]
    assert spy.call_args_list == calls


@skip_pypy
def test_instance_method_by_subclass_spy(mocker):
    class Base(object):
        def bar(self, arg):
            return arg * 2

    class Foo(Base):
        pass

    spy = mocker.spy(Foo, "bar")
    foo = Foo()
    other = Foo()
    assert foo.bar(arg=10) == 20
    assert other.bar(arg=10) == 20
    calls = [mocker.call(foo, arg=10), mocker.call(other, arg=10)]
    assert spy.call_args_list == calls
    assert spy.spy_return == 20


@skip_pypy
def test_class_method_spy(mocker):
    class Foo(object):
        @classmethod
        def bar(cls, arg):
            return arg * 2

    spy = mocker.spy(Foo, "bar")
    assert Foo.bar(arg=10) == 20
    Foo.bar.assert_called_once_with(arg=10)
    assert Foo.bar.spy_return == 20
    spy.assert_called_once_with(arg=10)
    assert spy.spy_return == 20


@skip_pypy
@pytest.mark.xfail(sys.version_info[0] == 2, reason="does not work on Python 2")
def test_class_method_subclass_spy(mocker):
    class Base(object):
        @classmethod
        def bar(self, arg):
            return arg * 2

    class Foo(Base):
        pass

    spy = mocker.spy(Foo, "bar")
    assert Foo.bar(arg=10) == 20
    Foo.bar.assert_called_once_with(arg=10)
    assert Foo.bar.spy_return == 20
    spy.assert_called_once_with(arg=10)
    assert spy.spy_return == 20


@skip_pypy
def test_class_method_with_metaclass_spy(mocker):
    class MetaFoo(type):
        pass

    class Foo(object):

        __metaclass__ = MetaFoo

        @classmethod
        def bar(cls, arg):
            return arg * 2

    spy = mocker.spy(Foo, "bar")
    assert Foo.bar(arg=10) == 20
    Foo.bar.assert_called_once_with(arg=10)
    assert Foo.bar.spy_return == 20
    spy.assert_called_once_with(arg=10)
    assert spy.spy_return == 20


@skip_pypy
def test_static_method_spy(mocker):
    class Foo(object):
        @staticmethod
        def bar(arg):
            return arg * 2

    spy = mocker.spy(Foo, "bar")
    assert Foo.bar(arg=10) == 20
    Foo.bar.assert_called_once_with(arg=10)
    assert Foo.bar.spy_return == 20
    spy.assert_called_once_with(arg=10)
    assert spy.spy_return == 20


@skip_pypy
@pytest.mark.xfail(sys.version_info[0] == 2, reason="does not work on Python 2")
def test_static_method_subclass_spy(mocker):
    class Base(object):
        @staticmethod
        def bar(arg):
            return arg * 2

    class Foo(Base):
        pass

    spy = mocker.spy(Foo, "bar")
    assert Foo.bar(arg=10) == 20
    Foo.bar.assert_called_once_with(arg=10)
    assert Foo.bar.spy_return == 20
    spy.assert_called_once_with(arg=10)
    assert spy.spy_return == 20


@pytest.mark.skip("Skip testdir")
def test_callable_like_spy(testdir, mocker):
    testdir.makepyfile(
        uut="""
        class CallLike(object):
            def __call__(self, x):
                return x * 2

        call_like = CallLike()
    """
    )
    testdir.syspathinsert()

    import uut

    spy = mocker.spy(uut, "call_like")
    uut.call_like(10)
    spy.assert_called_once_with(10)
    assert spy.spy_return == 20


@contextmanager
def assert_traceback():
    """
    Assert that this file is at the top of the filtered traceback
    """
    try:
        yield
    except AssertionError:
        traceback = py.code.ExceptionInfo().traceback
        crashentry = traceback.getcrashentry()
        assert crashentry.path == __file__
    else:
        raise AssertionError("DID NOT RAISE")


@contextmanager
def assert_argument_introspection(left, right):
    """
    Assert detailed argument introspection is used
    """
    try:
        yield
    except AssertionError as e:
        # this may be a bit too assuming, but seems nicer then hard-coding
        import _pytest.assertion.util as util

        # NOTE: we assert with either verbose or not, depending on how our own
        #       test was run by examining sys.argv
        verbose = any(a.startswith("-v") for a in sys.argv)
        expected = "\n  ".join(util._compare_eq_iterable(left, right, verbose))
        assert expected in str(e)
    else:
        raise AssertionError("DID NOT RAISE")


@pytest.mark.skipif(
    sys.version_info[:2] == (3, 4),
    reason="assert_not_called not available in Python 3.4",
)
def test_assert_not_called_wrapper(mocker):
    stub = mocker.stub()
    stub.assert_not_called()
    stub()
    with assert_traceback():
        stub.assert_not_called()


def test_assert_called_with_wrapper(mocker):
    stub = mocker.stub()
    stub("foo")
    stub.assert_called_with("foo")
    with assert_traceback():
        stub.assert_called_with("bar")


def test_assert_called_once_with_wrapper(mocker):
    stub = mocker.stub()
    stub("foo")
    stub.assert_called_once_with("foo")
    stub("foo")
    with assert_traceback():
        stub.assert_called_once_with("foo")


def test_assert_called_once_wrapper(mocker):
    stub = mocker.stub()
    if not hasattr(stub, "assert_called_once"):
        pytest.skip("assert_called_once not available")
    stub("foo")
    stub.assert_called_once()
    stub("foo")
    with assert_traceback():
        stub.assert_called_once()


def test_assert_called_wrapper(mocker):
    stub = mocker.stub()
    if not hasattr(stub, "assert_called"):
        pytest.skip("assert_called_once not available")
    with assert_traceback():
        stub.assert_called()
    stub("foo")
    stub.assert_called()
    stub("foo")
    stub.assert_called()


@pytest.mark.skip
@pytest.mark.usefixtures("needs_assert_rewrite")
def test_assert_called_args_with_introspection(mocker):
    stub = mocker.stub()

    complex_args = ("a", 1, set(["test"]))
    wrong_args = ("b", 2, set(["jest"]))

    stub(*complex_args)
    stub.assert_called_with(*complex_args)
    stub.assert_called_once_with(*complex_args)

    with assert_argument_introspection(complex_args, wrong_args):
        stub.assert_called_with(*wrong_args)
        stub.assert_called_once_with(*wrong_args)


@pytest.mark.skip
@pytest.mark.usefixtures("needs_assert_rewrite")
def test_assert_called_kwargs_with_introspection(mocker):
    stub = mocker.stub()

    complex_kwargs = dict(foo={"bar": 1, "baz": "spam"})
    wrong_kwargs = dict(foo={"goo": 1, "baz": "bran"})

    stub(**complex_kwargs)
    stub.assert_called_with(**complex_kwargs)
    stub.assert_called_once_with(**complex_kwargs)

    with assert_argument_introspection(complex_kwargs, wrong_kwargs):
        stub.assert_called_with(**wrong_kwargs)
        stub.assert_called_once_with(**wrong_kwargs)


def test_assert_any_call_wrapper(mocker):
    stub = mocker.stub()
    stub("foo")
    stub("foo")
    stub.assert_any_call("foo")
    with assert_traceback():
        stub.assert_any_call("bar")


def test_assert_has_calls(mocker):
    stub = mocker.stub()
    stub("foo")
    stub.assert_has_calls([mocker.call("foo")])
    with assert_traceback():
        stub.assert_has_calls([mocker.call("bar")])


@pytest.mark.skip("Skip testdir")
def test_monkeypatch_ini(mocker, testdir):
    # Make sure the following function actually tests something
    stub = mocker.stub()
    assert stub.assert_called_with.__module__ != stub.__module__

    testdir.makepyfile(
        """
        import py.code
        def test_foo(mocker):
            stub = mocker.stub()
            assert stub.assert_called_with.__module__ == stub.__module__
    """
    )
    testdir.makeini(
        """
        [pytest]
        mock_traceback_monkeypatch = false
    """
    )
    result = runpytest_subprocess(testdir)
    assert result.ret == 0


def test_parse_ini_boolean():
    import pytest_mock

    assert pytest_mock.parse_ini_boolean("True") is True
    assert pytest_mock.parse_ini_boolean("false") is False
    with pytest.raises(ValueError):
        pytest_mock.parse_ini_boolean("foo")


def test_patched_method_parameter_name(mocker):
    """Test that our internal code uses uncommon names when wrapping other
    "mock" methods to avoid conflicts with user code (#31).
    """

    class Request:
        @classmethod
        def request(cls, method, args):
            pass

    m = mocker.patch.object(Request, "request")
    Request.request(method="get", args={"type": "application/json"})
    m.assert_called_once_with(method="get", args={"type": "application/json"})


@pytest.mark.skip("Skip testdir")
def test_monkeypatch_native(testdir):
    """Automatically disable monkeypatching when --tb=native.
    """
    testdir.makepyfile(
        """
        def test_foo(mocker):
            stub = mocker.stub()
            stub(1, greet='hello')
            stub.assert_called_once_with(1, greet='hey')
    """
    )
    result = runpytest_subprocess(testdir, "--tb=native")
    assert result.ret == 1
    assert "During handling of the above exception" not in result.stdout.str()
    assert "Differing items:" not in result.stdout.str()
    traceback_lines = [
        x
        for x in result.stdout.str().splitlines()
        if "Traceback (most recent call last)" in x
    ]
    assert (
        len(traceback_lines) == 1
    )  # make sure there are no duplicated tracebacks (#44)


@pytest.mark.skip("Skip testdir")
def test_monkeypatch_no_terminal(testdir):
    """Don't crash without 'terminal' plugin.
    """
    testdir.makepyfile(
        """
        def test_foo(mocker):
            stub = mocker.stub()
            stub(1, greet='hello')
            stub.assert_called_once_with(1, greet='hey')
        """
    )
    result = runpytest_subprocess(testdir, "-p", "no:terminal", "-s")
    assert result.ret == 1
    assert result.stdout.lines == []


@pytest.mark.skip("Skip testdir")
@pytest.mark.skipif(sys.version_info[0] < 3, reason="Py3 only")
def test_standalone_mock(testdir):
    """Check that the "mock_use_standalone" is being used.
    """
    testdir.makepyfile(
        """
        def test_foo(mocker):
            pass
    """
    )
    testdir.makeini(
        """
        [pytest]
        mock_use_standalone_module = true
    """
    )
    result = runpytest_subprocess(testdir)
    assert result.ret == 3
    result.stderr.fnmatch_lines(["*No module named 'mock'*"])


def runpytest_subprocess(testdir, *args):
    """Testdir.runpytest_subprocess only available in pytest-2.8+"""
    if hasattr(testdir, "runpytest_subprocess"):
        return testdir.runpytest_subprocess(*args)
    else:
        # pytest 2.7.X
        return testdir.runpytest(*args)


@pytest.mark.skip("Skip testdir")
@pytest.mark.usefixtures("needs_assert_rewrite")
def test_detailed_introspection(testdir):
    """Check that the "mock_use_standalone" is being used.
    """
    testdir.makepyfile(
        """
        def test(mocker):
            m = mocker.Mock()
            m('fo')
            m.assert_called_once_with('', bar=4)
    """
    )
    result = testdir.runpytest("-s")
    if NEW_FORMATTING:
        expected_lines = [
            "*AssertionError: expected call not found.",
            "*Expected: mock('', bar=4)",
            "*Actual: mock('fo')",
        ]
    else:
        expected_lines = [
            "*AssertionError: Expected call: mock('', bar=4)*",
            "*Actual call: mock('fo')*",
        ]
    expected_lines += [
        "*pytest introspection follows:*",
        "*Args:",
        "*assert ('fo',) == ('',)",
        "*At index 0 diff: 'fo' != ''*",
        "*Use -v to get the full diff*",
        "*Kwargs:*",
        "*assert {} == {'bar': 4}*",
        "*Right contains* more item*",
        "*{'bar': 4}*",
        "*Use -v to get the full diff*",
    ]
    result.stdout.fnmatch_lines(expected_lines)


@pytest.mark.skip("Skip testdir")
def test_missing_introspection(testdir):
    testdir.makepyfile(
        """
        def test_foo(mocker):
            mock = mocker.Mock()
            mock('foo')
            mock('test')
            mock.assert_called_once_with('test')
    """
    )
    result = testdir.runpytest()
    assert "pytest introspection follows:" not in result.stdout.str()


def test_assert_called_with_unicode_arguments(mocker):
    """Test bug in assert_call_with called with non-ascii unicode string (#91)"""
    stub = mocker.stub()
    stub(b"l\xc3\xb6k".decode("UTF-8"))

    with pytest.raises(AssertionError):
        stub.assert_called_with(u"lak")


@pytest.mark.skip("Skip testdir")
def test_plain_stopall(testdir):
    """patch.stopall() in a test should not cause an error during unconfigure (#137)"""
    testdir.makepyfile(
        """
        import random

        def get_random_number():
            return random.randint(0, 100)

        def test_get_random_number(mocker):
            patcher = mocker.mock_module.patch("random.randint", lambda x, y: 5)
            patcher.start()
            assert get_random_number() == 5
            mocker.mock_module.patch.stopall()
    """
    )
    result = testdir.runpytest_subprocess()
    result.stdout.fnmatch_lines("* 1 passed in *")
    assert "RuntimeError" not in result.stderr.str()


def test_abort_patch_object_context_manager(mocker):
    class A(object):
        def doIt(self):
            return False

    a = A()

    with pytest.raises(ValueError) as excinfo:
        with mocker.patch.object(a, "doIt", return_value=True):
            assert a.doIt() == True

    expected_error_msg = (
        "Using mocker in a with context is not supported. "
        "https://github.com/pytest-dev/pytest-mock#note-about-usage-as-context-manager"
    )

    assert str(excinfo.value) == expected_error_msg


def test_abort_patch_context_manager(mocker):
    with pytest.raises(ValueError) as excinfo:
        with mocker.patch("some_package"):
            pass

    expected_error_msg = (
        "Using mocker in a with context is not supported. "
        "https://github.com/pytest-dev/pytest-mock#note-about-usage-as-context-manager"
    )

    assert str(excinfo.value) == expected_error_msg


@pytest.mark.skip("Skip testdir")
def test_abort_patch_context_manager_with_stale_pyc(testdir):
    """Ensure we don't trigger an error in case the frame where mocker.patch is being
    used doesn't have a 'context' (#169)"""
    import compileall

    py_fn = testdir.makepyfile(
        c="""
        class C:
            x = 1

        def check(mocker):
            mocker.patch.object(C, "x", 2)
            assert C.x == 2
    """
    )
    testdir.syspathinsert()

    testdir.makepyfile(
        """
        from c import check
        def test_foo(mocker):
            check(mocker)
    """
    )
    result = testdir.runpytest()
    result.stdout.fnmatch_lines("* 1 passed *")

    kwargs = {"legacy": True} if sys.version_info[0] >= 3 else {}
    assert compileall.compile_file(str(py_fn), **kwargs)

    pyc_fn = str(py_fn) + "c"
    assert os.path.isfile(pyc_fn)

    py_fn.remove()
    result = testdir.runpytest()
    result.stdout.fnmatch_lines("* 1 passed *")
