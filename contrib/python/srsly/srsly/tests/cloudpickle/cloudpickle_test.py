import _collections_abc
import abc
import collections
import base64
import functools
import io
import itertools
import logging
import math
import multiprocessing
from operator import itemgetter, attrgetter
import pickletools
import platform
import random
import re
import shutil
import subprocess
import sys
import tempfile
import textwrap
import types
import unittest
import weakref
import os
import enum
import typing
from functools import wraps

import pytest

try:
    # try importing numpy and scipy. These are not hard dependencies and
    # tests should be skipped if these modules are not available
    import numpy as np
    import scipy.special as spp
except (ImportError, RuntimeError):
    np = None
    spp = None

try:
    # Ditto for Tornado
    import tornado
except ImportError:
    tornado = None

import srsly.cloudpickle as cloudpickle
from srsly.cloudpickle.compat import pickle
from srsly.cloudpickle import register_pickle_by_value
from srsly.cloudpickle import unregister_pickle_by_value
from srsly.cloudpickle import list_registry_pickle_by_value
from srsly.cloudpickle.cloudpickle import _should_pickle_by_reference
from srsly.cloudpickle.cloudpickle import _make_empty_cell, cell_set
from srsly.cloudpickle.cloudpickle import _extract_class_dict, _whichmodule
from srsly.cloudpickle.cloudpickle import _lookup_module_and_qualname

from .testutils import subprocess_pickle_echo
from .testutils import subprocess_pickle_string
from .testutils import assert_run_python_script
from .testutils import subprocess_worker


_TEST_GLOBAL_VARIABLE = "default_value"
_TEST_GLOBAL_VARIABLE2 = "another_value"


class RaiserOnPickle:

    def __init__(self, exc):
        self.exc = exc

    def __reduce__(self):
        raise self.exc


def pickle_depickle(obj, protocol=cloudpickle.DEFAULT_PROTOCOL):
    """Helper function to test whether object pickled with cloudpickle can be
    depickled with pickle
    """
    return pickle.loads(cloudpickle.dumps(obj, protocol=protocol))


def _escape(raw_filepath):
    # Ugly hack to embed filepaths in code templates for windows
    return raw_filepath.replace("\\", r"\\\\")


def _maybe_remove(list_, item):
    try:
        list_.remove(item)
    except ValueError:
        pass
    return list_


def test_extract_class_dict():
    class A(int):
        """A docstring"""
        def method(self):
            return "a"

    class B:
        """B docstring"""
        B_CONSTANT = 42

        def method(self):
            return "b"

    class C(A, B):
        C_CONSTANT = 43

        def method_c(self):
            return "c"

    clsdict = _extract_class_dict(C)
    assert sorted(clsdict.keys()) == ["C_CONSTANT", "__doc__", "method_c"]
    assert clsdict["C_CONSTANT"] == 43
    assert clsdict["__doc__"] is None
    assert clsdict["method_c"](C()) == C().method_c()


class CloudPickleTest(unittest.TestCase):

    protocol = cloudpickle.DEFAULT_PROTOCOL

    def setUp(self):
        self.tmpdir = tempfile.mkdtemp(prefix="tmp_cloudpickle_test_")

    def tearDown(self):
        shutil.rmtree(self.tmpdir)

    @pytest.mark.skipif(
            platform.python_implementation() != "CPython" or
            (sys.version_info >= (3, 8, 0) and sys.version_info < (3, 8, 2)),
            reason="Underlying bug fixed upstream starting Python 3.8.2")
    def test_reducer_override_reference_cycle(self):
        # Early versions of Python 3.8 introduced a reference cycle between a
        # Pickler and it's reducer_override method. Because a Pickler
        # object references every object it has pickled through its memo, this
        # cycle prevented the garbage-collection of those external pickled
        # objects. See #327 as well as https://bugs.python.org/issue39492
        # This bug was fixed in Python 3.8.2, but is still present using
        # cloudpickle and Python 3.8.0/1, hence the skipif directive.
        class MyClass:
            pass

        my_object = MyClass()
        wr = weakref.ref(my_object)

        cloudpickle.dumps(my_object)
        del my_object
        assert wr() is None, "'del'-ed my_object has not been collected"

    def test_itemgetter(self):
        d = range(10)
        getter = itemgetter(1)

        getter2 = pickle_depickle(getter, protocol=self.protocol)
        self.assertEqual(getter(d), getter2(d))

        getter = itemgetter(0, 3)
        getter2 = pickle_depickle(getter, protocol=self.protocol)
        self.assertEqual(getter(d), getter2(d))

    def test_attrgetter(self):
        class C:
            def __getattr__(self, item):
                return item
        d = C()
        getter = attrgetter("a")
        getter2 = pickle_depickle(getter, protocol=self.protocol)
        self.assertEqual(getter(d), getter2(d))
        getter = attrgetter("a", "b")
        getter2 = pickle_depickle(getter, protocol=self.protocol)
        self.assertEqual(getter(d), getter2(d))

        d.e = C()
        getter = attrgetter("e.a")
        getter2 = pickle_depickle(getter, protocol=self.protocol)
        self.assertEqual(getter(d), getter2(d))
        getter = attrgetter("e.a", "e.b")
        getter2 = pickle_depickle(getter, protocol=self.protocol)
        self.assertEqual(getter(d), getter2(d))

    # Regression test for SPARK-3415
    @pytest.mark.skip(reason="Requires pytest -s to pass")
    def test_pickling_file_handles(self):
        out1 = sys.stderr
        out2 = pickle.loads(cloudpickle.dumps(out1, protocol=self.protocol))
        self.assertEqual(out1, out2)

    def test_func_globals(self):
        class Unpicklable:
            def __reduce__(self):
                raise Exception("not picklable")

        global exit
        exit = Unpicklable()

        self.assertRaises(Exception, lambda: cloudpickle.dumps(
            exit, protocol=self.protocol))

        def foo():
            sys.exit(0)

        self.assertTrue("exit" in foo.__code__.co_names)
        cloudpickle.dumps(foo)

    def test_buffer(self):
        try:
            buffer_obj = buffer("Hello")
            buffer_clone = pickle_depickle(buffer_obj, protocol=self.protocol)
            self.assertEqual(buffer_clone, str(buffer_obj))
            buffer_obj = buffer("Hello", 2, 3)
            buffer_clone = pickle_depickle(buffer_obj, protocol=self.protocol)
            self.assertEqual(buffer_clone, str(buffer_obj))
        except NameError:  # Python 3 does no longer support buffers
            pass

    def test_memoryview(self):
        buffer_obj = memoryview(b"Hello")
        self.assertEqual(pickle_depickle(buffer_obj, protocol=self.protocol),
                         buffer_obj.tobytes())

    def test_dict_keys(self):
        keys = {"a": 1, "b": 2}.keys()
        results = pickle_depickle(keys)
        self.assertEqual(results, keys)
        assert isinstance(results, _collections_abc.dict_keys)

    def test_dict_values(self):
        values = {"a": 1, "b": 2}.values()
        results = pickle_depickle(values)
        self.assertEqual(sorted(results), sorted(values))
        assert isinstance(results, _collections_abc.dict_values)

    def test_dict_items(self):
        items = {"a": 1, "b": 2}.items()
        results = pickle_depickle(items)
        self.assertEqual(results, items)
        assert isinstance(results, _collections_abc.dict_items)

    def test_odict_keys(self):
        keys = collections.OrderedDict([("a", 1), ("b", 2)]).keys()
        results = pickle_depickle(keys)
        self.assertEqual(results, keys)
        assert type(keys) == type(results)

    def test_odict_values(self):
        values = collections.OrderedDict([("a", 1), ("b", 2)]).values()
        results = pickle_depickle(values)
        self.assertEqual(list(results), list(values))
        assert type(values) == type(results)

    def test_odict_items(self):
        items = collections.OrderedDict([("a", 1), ("b", 2)]).items()
        results = pickle_depickle(items)
        self.assertEqual(results, items)
        assert type(items) == type(results)

    def test_sliced_and_non_contiguous_memoryview(self):
        buffer_obj = memoryview(b"Hello!" * 3)[2:15:2]
        self.assertEqual(pickle_depickle(buffer_obj, protocol=self.protocol),
                         buffer_obj.tobytes())

    def test_large_memoryview(self):
        buffer_obj = memoryview(b"Hello!" * int(1e7))
        self.assertEqual(pickle_depickle(buffer_obj, protocol=self.protocol),
                         buffer_obj.tobytes())

    def test_lambda(self):
        self.assertEqual(
                pickle_depickle(lambda: 1, protocol=self.protocol)(), 1)

    def test_nested_lambdas(self):
        a, b = 1, 2
        f1 = lambda x: x + a
        f2 = lambda x: f1(x) // b
        self.assertEqual(pickle_depickle(f2, protocol=self.protocol)(1), 1)

    def test_recursive_closure(self):
        def f1():
            def g():
                return g
            return g

        def f2(base):
            def g(n):
                return base if n <= 1 else n * g(n - 1)
            return g

        g1 = pickle_depickle(f1(), protocol=self.protocol)
        self.assertEqual(g1(), g1)

        g2 = pickle_depickle(f2(2), protocol=self.protocol)
        self.assertEqual(g2(5), 240)

    def test_closure_none_is_preserved(self):
        def f():
            """a function with no closure cells
            """

        self.assertTrue(
            f.__closure__ is None,
            msg='f actually has closure cells!',
        )

        g = pickle_depickle(f, protocol=self.protocol)

        self.assertTrue(
            g.__closure__ is None,
            msg='g now has closure cells even though f does not',
        )

    def test_empty_cell_preserved(self):
        def f():
            if False:  # pragma: no cover
                cell = None

            def g():
                cell  # NameError, unbound free variable

            return g

        g1 = f()
        with pytest.raises(NameError):
            g1()

        g2 = pickle_depickle(g1, protocol=self.protocol)
        with pytest.raises(NameError):
            g2()

    def test_unhashable_closure(self):
        def f():
            s = {1, 2}  # mutable set is unhashable

            def g():
                return len(s)

            return g

        g = pickle_depickle(f(), protocol=self.protocol)
        self.assertEqual(g(), 2)

    def test_dynamically_generated_class_that_uses_super(self):

        class Base:
            def method(self):
                return 1

        class Derived(Base):
            "Derived Docstring"
            def method(self):
                return super().method() + 1

        self.assertEqual(Derived().method(), 2)

        # Pickle and unpickle the class.
        UnpickledDerived = pickle_depickle(Derived, protocol=self.protocol)
        self.assertEqual(UnpickledDerived().method(), 2)

        # We have special logic for handling __doc__ because it's a readonly
        # attribute on PyPy.
        self.assertEqual(UnpickledDerived.__doc__, "Derived Docstring")

        # Pickle and unpickle an instance.
        orig_d = Derived()
        d = pickle_depickle(orig_d, protocol=self.protocol)
        self.assertEqual(d.method(), 2)

    def test_cycle_in_classdict_globals(self):

        class C:

            def it_works(self):
                return "woohoo!"

        C.C_again = C
        C.instance_of_C = C()

        depickled_C = pickle_depickle(C, protocol=self.protocol)
        depickled_instance = pickle_depickle(C())

        # Test instance of depickled class.
        self.assertEqual(depickled_C().it_works(), "woohoo!")
        self.assertEqual(depickled_C.C_again().it_works(), "woohoo!")
        self.assertEqual(depickled_C.instance_of_C.it_works(), "woohoo!")
        self.assertEqual(depickled_instance.it_works(), "woohoo!")

    def test_locally_defined_function_and_class(self):
        LOCAL_CONSTANT = 42

        def some_function(x, y):
            # Make sure the __builtins__ are not broken (see #211)
            sum(range(10))
            return (x + y) / LOCAL_CONSTANT

        # pickle the function definition
        self.assertEqual(pickle_depickle(some_function, protocol=self.protocol)(41, 1), 1)
        self.assertEqual(pickle_depickle(some_function, protocol=self.protocol)(81, 3), 2)

        hidden_constant = lambda: LOCAL_CONSTANT

        class SomeClass:
            """Overly complicated class with nested references to symbols"""
            def __init__(self, value):
                self.value = value

            def one(self):
                return LOCAL_CONSTANT / hidden_constant()

            def some_method(self, x):
                return self.one() + some_function(x, 1) + self.value

        # pickle the class definition
        clone_class = pickle_depickle(SomeClass, protocol=self.protocol)
        self.assertEqual(clone_class(1).one(), 1)
        self.assertEqual(clone_class(5).some_method(41), 7)
        clone_class = subprocess_pickle_echo(SomeClass, protocol=self.protocol)
        self.assertEqual(clone_class(5).some_method(41), 7)

        # pickle the class instances
        self.assertEqual(pickle_depickle(SomeClass(1)).one(), 1)
        self.assertEqual(pickle_depickle(SomeClass(5)).some_method(41), 7)
        new_instance = subprocess_pickle_echo(SomeClass(5),
                                              protocol=self.protocol)
        self.assertEqual(new_instance.some_method(41), 7)

        # pickle the method instances
        self.assertEqual(pickle_depickle(SomeClass(1).one)(), 1)
        self.assertEqual(pickle_depickle(SomeClass(5).some_method)(41), 7)
        new_method = subprocess_pickle_echo(SomeClass(5).some_method,
                                            protocol=self.protocol)
        self.assertEqual(new_method(41), 7)

    def test_partial(self):
        partial_obj = functools.partial(min, 1)
        partial_clone = pickle_depickle(partial_obj, protocol=self.protocol)
        self.assertEqual(partial_clone(4), 1)

    @pytest.mark.skipif(platform.python_implementation() == 'PyPy',
                        reason="Skip numpy and scipy tests on PyPy")
    def test_ufunc(self):
        # test a numpy ufunc (universal function), which is a C-based function
        # that is applied on a numpy array

        if np:
            # simple ufunc: np.add
            self.assertEqual(pickle_depickle(np.add, protocol=self.protocol),
                             np.add)
        else:  # skip if numpy is not available
            pass

        if spp:
            # custom ufunc: scipy.special.iv
            self.assertEqual(pickle_depickle(spp.iv, protocol=self.protocol),
                             spp.iv)
        else:  # skip if scipy is not available
            pass

    def test_loads_namespace(self):
        obj = 1, 2, 3, 4
        returned_obj = cloudpickle.loads(cloudpickle.dumps(
            obj, protocol=self.protocol))
        self.assertEqual(obj, returned_obj)

    def test_load_namespace(self):
        obj = 1, 2, 3, 4
        bio = io.BytesIO()
        cloudpickle.dump(obj, bio)
        bio.seek(0)
        returned_obj = cloudpickle.load(bio)
        self.assertEqual(obj, returned_obj)

    def test_generator(self):

        def some_generator(cnt):
            for i in range(cnt):
                yield i

        gen2 = pickle_depickle(some_generator, protocol=self.protocol)

        assert type(gen2(3)) == type(some_generator(3))
        assert list(gen2(3)) == list(range(3))

    def test_classmethod(self):
        class A:
            @staticmethod
            def test_sm():
                return "sm"
            @classmethod
            def test_cm(cls):
                return "cm"

        sm = A.__dict__["test_sm"]
        cm = A.__dict__["test_cm"]

        A.test_sm = pickle_depickle(sm, protocol=self.protocol)
        A.test_cm = pickle_depickle(cm, protocol=self.protocol)

        self.assertEqual(A.test_sm(), "sm")
        self.assertEqual(A.test_cm(), "cm")

    def test_bound_classmethod(self):
        class A:
            @classmethod
            def test_cm(cls):
                return "cm"

        A.test_cm = pickle_depickle(A.test_cm, protocol=self.protocol)
        self.assertEqual(A.test_cm(), "cm")

    def test_method_descriptors(self):
        f = pickle_depickle(str.upper)
        self.assertEqual(f('abc'), 'ABC')

    def test_instancemethods_without_self(self):
        class F:
            def f(self, x):
                return x + 1

        g = pickle_depickle(F.f, protocol=self.protocol)
        self.assertEqual(g.__name__, F.f.__name__)
        # self.assertEqual(g(F(), 1), 2)  # still fails

    def test_module(self):
        pickle_clone = pickle_depickle(pickle, protocol=self.protocol)
        self.assertEqual(pickle, pickle_clone)

    def test_dynamic_module(self):
        mod = types.ModuleType('mod')
        code = '''
        x = 1
        def f(y):
            return x + y

        class Foo:
            def method(self, x):
                return f(x)
        '''
        exec(textwrap.dedent(code), mod.__dict__)
        mod2 = pickle_depickle(mod, protocol=self.protocol)
        self.assertEqual(mod.x, mod2.x)
        self.assertEqual(mod.f(5), mod2.f(5))
        self.assertEqual(mod.Foo().method(5), mod2.Foo().method(5))

        if platform.python_implementation() != 'PyPy':
            # XXX: this fails with excessive recursion on PyPy.
            mod3 = subprocess_pickle_echo(mod, protocol=self.protocol)
            self.assertEqual(mod.x, mod3.x)
            self.assertEqual(mod.f(5), mod3.f(5))
            self.assertEqual(mod.Foo().method(5), mod3.Foo().method(5))

        # Test dynamic modules when imported back are singletons
        mod1, mod2 = pickle_depickle([mod, mod])
        self.assertEqual(id(mod1), id(mod2))

        # Ensure proper pickling of mod's functions when module "looks" like a
        # file-backed module even though it is not:
        try:
            sys.modules['mod'] = mod
            depickled_f = pickle_depickle(mod.f, protocol=self.protocol)
            self.assertEqual(mod.f(5), depickled_f(5))
        finally:
            sys.modules.pop('mod', None)

    def test_module_locals_behavior(self):
        # Makes sure that a local function defined in another module is
        # correctly serialized. This notably checks that the globals are
        # accessible and that there is no issue with the builtins (see #211)

        pickled_func_path = os.path.join(self.tmpdir, 'local_func_g.pkl')

        child_process_script = '''
        from srsly.cloudpickle.compat import pickle
        import gc
        with open("{pickled_func_path}", 'rb') as f:
            func = pickle.load(f)

        assert func(range(10)) == 45
        '''

        child_process_script = child_process_script.format(
                pickled_func_path=_escape(pickled_func_path))

        try:

            from srsly.tests.cloudpickle.testutils import make_local_function

            g = make_local_function()
            with open(pickled_func_path, 'wb') as f:
                cloudpickle.dump(g, f, protocol=self.protocol)

            assert_run_python_script(textwrap.dedent(child_process_script))

        finally:
            os.unlink(pickled_func_path)

    def test_dynamic_module_with_unpicklable_builtin(self):
        # Reproducer of https://github.com/cloudpipe/cloudpickle/issues/316
        # Some modules such as scipy inject some unpicklable objects into the
        # __builtins__ module, which appears in every module's __dict__ under
        # the '__builtins__' key. In such cases, cloudpickle used to fail
        # when pickling dynamic modules.
        class UnpickleableObject:
            def __reduce__(self):
                raise ValueError('Unpicklable object')

        mod = types.ModuleType("mod")

        exec('f = lambda x: abs(x)', mod.__dict__)
        assert mod.f(-1) == 1
        assert '__builtins__' in mod.__dict__

        unpicklable_obj = UnpickleableObject()
        with pytest.raises(ValueError):
            cloudpickle.dumps(unpicklable_obj)

        # Emulate the behavior of scipy by injecting an unpickleable object
        # into mod's builtins.
        # The __builtins__ entry of mod's __dict__ can either be the
        # __builtins__ module, or the __builtins__ module's __dict__. #316
        # happens only in the latter case.
        if isinstance(mod.__dict__['__builtins__'], dict):
            mod.__dict__['__builtins__']['unpickleable_obj'] = unpicklable_obj
        elif isinstance(mod.__dict__['__builtins__'], types.ModuleType):
            mod.__dict__['__builtins__'].unpickleable_obj = unpicklable_obj

        depickled_mod = pickle_depickle(mod, protocol=self.protocol)
        assert '__builtins__' in depickled_mod.__dict__

        if isinstance(depickled_mod.__dict__['__builtins__'], dict):
            assert "abs" in depickled_mod.__builtins__
        elif isinstance(
                depickled_mod.__dict__['__builtins__'], types.ModuleType):
            assert hasattr(depickled_mod.__builtins__, "abs")
        assert depickled_mod.f(-1) == 1

        # Additional check testing that the issue #425 is fixed: without the
        # fix for #425, `mod.f` would not have access to `__builtins__`, and
        # thus calling `mod.f(-1)` (which relies on the `abs` builtin) would
        # fail.
        assert mod.f(-1) == 1

    def test_load_dynamic_module_in_grandchild_process(self):
        # Make sure that when loaded, a dynamic module preserves its dynamic
        # property. Otherwise, this will lead to an ImportError if pickled in
        # the child process and reloaded in another one.

        # We create a new dynamic module
        mod = types.ModuleType('mod')
        code = '''
        x = 1
        '''
        exec(textwrap.dedent(code), mod.__dict__)

        # This script will be ran in a separate child process. It will import
        # the pickled dynamic module, and then re-pickle it under a new name.
        # Finally, it will create a child process that will load the re-pickled
        # dynamic module.
        parent_process_module_file = os.path.join(
            self.tmpdir, 'dynamic_module_from_parent_process.pkl')
        child_process_module_file = os.path.join(
            self.tmpdir, 'dynamic_module_from_child_process.pkl')
        child_process_script = '''
            from srsly.cloudpickle.compat import pickle
            import textwrap

            import srsly.cloudpickle as cloudpickle
            from srsly.tests.cloudpickle.testutils import assert_run_python_script


            child_of_child_process_script = {child_of_child_process_script}

            with open('{parent_process_module_file}', 'rb') as f:
                mod = pickle.load(f)

            with open('{child_process_module_file}', 'wb') as f:
                cloudpickle.dump(mod, f, protocol={protocol})

            assert_run_python_script(textwrap.dedent(child_of_child_process_script))
            '''

        # The script ran by the process created by the child process
        child_of_child_process_script = """ '''
                from srsly.cloudpickle.compat import pickle
                with open('{child_process_module_file}','rb') as fid:
                    mod = pickle.load(fid)
                ''' """

        # Filling the two scripts with the pickled modules filepaths and,
        # for the first child process, the script to be executed by its
        # own child process.
        child_of_child_process_script = child_of_child_process_script.format(
                child_process_module_file=child_process_module_file)

        child_process_script = child_process_script.format(
            parent_process_module_file=_escape(parent_process_module_file),
            child_process_module_file=_escape(child_process_module_file),
            child_of_child_process_script=_escape(child_of_child_process_script),
            protocol=self.protocol)

        try:
            with open(parent_process_module_file, 'wb') as fid:
                cloudpickle.dump(mod, fid, protocol=self.protocol)

            assert_run_python_script(textwrap.dedent(child_process_script))

        finally:
            # Remove temporary created files
            if os.path.exists(parent_process_module_file):
                os.unlink(parent_process_module_file)
            if os.path.exists(child_process_module_file):
                os.unlink(child_process_module_file)

    def test_correct_globals_import(self):
        def nested_function(x):
            return x + 1

        def unwanted_function(x):
            return math.exp(x)

        def my_small_function(x, y):
            return nested_function(x) + y

        b = cloudpickle.dumps(my_small_function, protocol=self.protocol)

        # Make sure that the pickle byte string only includes the definition
        # of my_small_function and its dependency nested_function while
        # extra functions and modules such as unwanted_function and the math
        # module are not included so as to keep the pickle payload as
        # lightweight as possible.

        assert b'my_small_function' in b
        assert b'nested_function' in b

        assert b'unwanted_function' not in b
        assert b'math' not in b

    def test_module_importability(self):
        pytest.importorskip("_cloudpickle_testpkg")
        from srsly.cloudpickle.compat import pickle
        import os.path
        import collections
        import collections.abc

        assert _should_pickle_by_reference(pickle)
        assert _should_pickle_by_reference(os.path)  # fake (aliased) module
        assert _should_pickle_by_reference(collections)  # package
        assert _should_pickle_by_reference(collections.abc)  # module in package

        dynamic_module = types.ModuleType('dynamic_module')
        assert not _should_pickle_by_reference(dynamic_module)

        if platform.python_implementation() == 'PyPy':
            import _codecs
            assert _should_pickle_by_reference(_codecs)

        # #354: Check that modules created dynamically during the import of
        # their parent modules are considered importable by cloudpickle.
        # See the mod_with_dynamic_submodule documentation for more
        # details of this use case.
        import _cloudpickle_testpkg.mod.dynamic_submodule as m
        assert _should_pickle_by_reference(m)
        assert pickle_depickle(m, protocol=self.protocol) is m

        # Check for similar behavior for a module that cannot be imported by
        # attribute lookup.
        from _cloudpickle_testpkg.mod import dynamic_submodule_two as m2
        # Note: import _cloudpickle_testpkg.mod.dynamic_submodule_two as m2
        # works only for Python 3.7+
        assert _should_pickle_by_reference(m2)
        assert pickle_depickle(m2, protocol=self.protocol) is m2

        # Submodule_three is a dynamic module only importable via module lookup
        with pytest.raises(ImportError):
            import _cloudpickle_testpkg.mod.submodule_three  # noqa
        from _cloudpickle_testpkg.mod import submodule_three as m3
        assert not _should_pickle_by_reference(m3)

        # This module cannot be pickled using attribute lookup (as it does not
        # have a `__module__` attribute like classes and functions.
        assert not hasattr(m3, '__module__')
        depickled_m3 = pickle_depickle(m3, protocol=self.protocol)
        assert depickled_m3 is not m3
        assert m3.f(1) == depickled_m3.f(1)

        # Do the same for an importable dynamic submodule inside a dynamic
        # module inside a file-backed module.
        import _cloudpickle_testpkg.mod.dynamic_submodule.dynamic_subsubmodule as sm  # noqa
        assert _should_pickle_by_reference(sm)
        assert pickle_depickle(sm, protocol=self.protocol) is sm

        expected = "cannot check importability of object instances"
        with pytest.raises(TypeError, match=expected):
            _should_pickle_by_reference(object())

    def test_Ellipsis(self):
        self.assertEqual(Ellipsis,
                         pickle_depickle(Ellipsis, protocol=self.protocol))

    def test_NotImplemented(self):
        ExcClone = pickle_depickle(NotImplemented, protocol=self.protocol)
        self.assertEqual(NotImplemented, ExcClone)

    def test_NoneType(self):
        res = pickle_depickle(type(None), protocol=self.protocol)
        self.assertEqual(type(None), res)

    def test_EllipsisType(self):
        res = pickle_depickle(type(Ellipsis), protocol=self.protocol)
        self.assertEqual(type(Ellipsis), res)

    def test_NotImplementedType(self):
        res = pickle_depickle(type(NotImplemented), protocol=self.protocol)
        self.assertEqual(type(NotImplemented), res)

    def test_builtin_function(self):
        # Note that builtin_function_or_method are special-cased by cloudpickle
        # only in python2.

        # builtin function from the __builtin__ module
        assert pickle_depickle(zip, protocol=self.protocol) is zip

        from os import mkdir
        # builtin function from a "regular" module
        assert pickle_depickle(mkdir, protocol=self.protocol) is mkdir

    def test_builtin_type_constructor(self):
        # This test makes sure that cloudpickling builtin-type
        # constructors works for all python versions/implementation.

        # pickle_depickle some builtin methods of the __builtin__ module
        for t in list, tuple, set, frozenset, dict, object:
            cloned_new = pickle_depickle(t.__new__, protocol=self.protocol)
            assert isinstance(cloned_new(t), t)

    # The next 4 tests cover all cases into which builtin python methods can
    # appear.
    # There are 4 kinds of method: 'classic' methods, classmethods,
    # staticmethods and slotmethods. They will appear under different types
    # depending on whether they are called from the __dict__ of their
    # class, their class itself, or an instance of their class. This makes
    # 12 total combinations.
    # This discussion and the following tests are relevant for the CPython
    # implementation only. In PyPy, there is no builtin method or builtin
    # function types/flavours. The only way into which a builtin method can be
    # identified is with it's builtin-code __code__ attribute.

    def test_builtin_classicmethod(self):
        obj = 1.5  # float object

        bound_classicmethod = obj.hex  # builtin_function_or_method
        unbound_classicmethod = type(obj).hex  # method_descriptor
        clsdict_classicmethod = type(obj).__dict__['hex']  # method_descriptor

        assert unbound_classicmethod is clsdict_classicmethod

        depickled_bound_meth = pickle_depickle(
            bound_classicmethod, protocol=self.protocol)
        depickled_unbound_meth = pickle_depickle(
            unbound_classicmethod, protocol=self.protocol)
        depickled_clsdict_meth = pickle_depickle(
            clsdict_classicmethod, protocol=self.protocol)

        # No identity on the bound methods they are bound to different float
        # instances
        assert depickled_bound_meth() == bound_classicmethod()
        assert depickled_unbound_meth is unbound_classicmethod
        assert depickled_clsdict_meth is clsdict_classicmethod


    @pytest.mark.skipif(
        (platform.machine() == "aarch64" and sys.version_info[:2] >= (3, 10))
            or platform.python_implementation() == "PyPy"
            or (sys.version_info[:2] == (3, 10) and sys.version_info >= (3, 10, 8))
            # Skipping tests on 3.11 due to https://github.com/cloudpipe/cloudpickle/pull/486.
            or sys.version_info[:2] >= (3, 11),
        reason="Fails on aarch64 + python 3.10+ in cibuildwheel, currently unable to replicate failure elsewhere; fails sometimes for pypy on conda-forge; fails for python 3.10.8+ and 3.11+")
    def test_builtin_classmethod(self):
        obj = 1.5  # float object

        bound_clsmethod = obj.fromhex  # builtin_function_or_method
        unbound_clsmethod = type(obj).fromhex  # builtin_function_or_method
        clsdict_clsmethod = type(
            obj).__dict__['fromhex']  # classmethod_descriptor

        depickled_bound_meth = pickle_depickle(
            bound_clsmethod, protocol=self.protocol)
        depickled_unbound_meth = pickle_depickle(
            unbound_clsmethod, protocol=self.protocol)
        depickled_clsdict_meth = pickle_depickle(
            clsdict_clsmethod, protocol=self.protocol)

        # float.fromhex takes a string as input.
        arg = "0x1"

        # Identity on both the bound and the unbound methods cannot be
        # tested: the bound methods are bound to different objects, and the
        # unbound methods are actually recreated at each call.
        assert depickled_bound_meth(arg) == bound_clsmethod(arg)
        assert depickled_unbound_meth(arg) == unbound_clsmethod(arg)

        if platform.python_implementation() == 'CPython':
            # Roundtripping a classmethod_descriptor results in a
            # builtin_function_or_method (CPython upstream issue).
            assert depickled_clsdict_meth(arg) == clsdict_clsmethod(float, arg)
        if platform.python_implementation() == 'PyPy':
            # builtin-classmethods are simple classmethod in PyPy (not
            # callable). We test equality of types and the functionality of the
            # __func__ attribute instead. We do not test the the identity of
            # the functions as __func__ attributes of classmethods are not
            # pickleable and must be reconstructed at depickling time.
            assert type(depickled_clsdict_meth) == type(clsdict_clsmethod)
            assert depickled_clsdict_meth.__func__(
                float, arg) == clsdict_clsmethod.__func__(float, arg)

    def test_builtin_slotmethod(self):
        obj = 1.5  # float object

        bound_slotmethod = obj.__repr__  # method-wrapper
        unbound_slotmethod = type(obj).__repr__  # wrapper_descriptor
        clsdict_slotmethod = type(obj).__dict__['__repr__']  # ditto

        depickled_bound_meth = pickle_depickle(
            bound_slotmethod, protocol=self.protocol)
        depickled_unbound_meth = pickle_depickle(
            unbound_slotmethod, protocol=self.protocol)
        depickled_clsdict_meth = pickle_depickle(
            clsdict_slotmethod, protocol=self.protocol)

        # No identity tests on the bound slotmethod are they are bound to
        # different float instances
        assert depickled_bound_meth() == bound_slotmethod()
        assert depickled_unbound_meth is unbound_slotmethod
        assert depickled_clsdict_meth is clsdict_slotmethod

    @pytest.mark.skipif(
        platform.python_implementation() == "PyPy",
        reason="No known staticmethod example in the pypy stdlib")
    def test_builtin_staticmethod(self):
        obj = "foo"  # str object

        bound_staticmethod = obj.maketrans  # builtin_function_or_method
        unbound_staticmethod = type(obj).maketrans  # ditto
        clsdict_staticmethod = type(obj).__dict__['maketrans']  # staticmethod

        assert bound_staticmethod is unbound_staticmethod

        depickled_bound_meth = pickle_depickle(
            bound_staticmethod, protocol=self.protocol)
        depickled_unbound_meth = pickle_depickle(
            unbound_staticmethod, protocol=self.protocol)
        depickled_clsdict_meth = pickle_depickle(
            clsdict_staticmethod, protocol=self.protocol)

        assert depickled_bound_meth is bound_staticmethod
        assert depickled_unbound_meth is unbound_staticmethod

        # staticmethod objects are recreated at depickling time, but the
        # underlying __func__ object is pickled by attribute.
        assert depickled_clsdict_meth.__func__ is clsdict_staticmethod.__func__
        type(depickled_clsdict_meth) is type(clsdict_staticmethod)

    @pytest.mark.skipif(tornado is None,
                        reason="test needs Tornado installed")
    def test_tornado_coroutine(self):
        # Pickling a locally defined coroutine function
        from tornado import gen, ioloop

        @gen.coroutine
        def f(x, y):
            yield gen.sleep(x)
            raise gen.Return(y + 1)

        @gen.coroutine
        def g(y):
            res = yield f(0.01, y)
            raise gen.Return(res + 1)

        data = cloudpickle.dumps([g, g], protocol=self.protocol)
        f = g = None
        g2, g3 = pickle.loads(data)
        self.assertTrue(g2 is g3)
        loop = ioloop.IOLoop.current()
        res = loop.run_sync(functools.partial(g2, 5))
        self.assertEqual(res, 7)

    @pytest.mark.skipif(
        (3, 11, 0, 'beta') <= sys.version_info < (3, 11, 0, 'beta', 4),
        reason="https://github.com/python/cpython/issues/92932"
    )
    def test_extended_arg(self):
        # Functions with more than 65535 global vars prefix some global
        # variable references with the EXTENDED_ARG opcode.
        nvars = 65537 + 258
        names = ['g%d' % i for i in range(1, nvars)]
        r = random.Random(42)
        d = {name: r.randrange(100) for name in names}
        # def f(x):
        #     x = g1, g2, ...
        #     return zlib.crc32(bytes(bytearray(x)))
        code = """
        import zlib

        def f():
            x = {tup}
            return zlib.crc32(bytes(bytearray(x)))
        """.format(tup=', '.join(names))
        exec(textwrap.dedent(code), d, d)
        f = d['f']
        res = f()
        data = cloudpickle.dumps([f, f], protocol=self.protocol)
        d = f = None
        f2, f3 = pickle.loads(data)
        self.assertTrue(f2 is f3)
        self.assertEqual(f2(), res)

    def test_submodule(self):
        # Function that refers (by attribute) to a sub-module of a package.

        # Choose any module NOT imported by __init__ of its parent package
        # examples in standard library include:
        # - http.cookies, unittest.mock, curses.textpad, xml.etree.ElementTree

        global xml # imitate performing this import at top of file
        import xml.etree.ElementTree
        def example():
            x = xml.etree.ElementTree.Comment # potential AttributeError

        s = cloudpickle.dumps(example, protocol=self.protocol)

        # refresh the environment, i.e., unimport the dependency
        del xml
        for item in list(sys.modules):
            if item.split('.')[0] == 'xml':
                del sys.modules[item]

        # deserialise
        f = pickle.loads(s)
        f() # perform test for error

    def test_submodule_closure(self):
        # Same as test_submodule except the package is not a global
        def scope():
            import xml.etree.ElementTree
            def example():
                x = xml.etree.ElementTree.Comment # potential AttributeError
            return example
        example = scope()

        s = cloudpickle.dumps(example, protocol=self.protocol)

        # refresh the environment (unimport dependency)
        for item in list(sys.modules):
            if item.split('.')[0] == 'xml':
                del sys.modules[item]

        f = cloudpickle.loads(s)
        f() # test

    def test_multiprocess(self):
        # running a function pickled by another process (a la dask.distributed)
        def scope():
            def example():
                x = xml.etree.ElementTree.Comment
            return example
        global xml
        import xml.etree.ElementTree
        example = scope()

        s = cloudpickle.dumps(example, protocol=self.protocol)

        # choose "subprocess" rather than "multiprocessing" because the latter
        # library uses fork to preserve the parent environment.
        command = ("import base64; "
                   "from srsly.cloudpickle.compat import pickle; "
                   "pickle.loads(base64.b32decode('" +
                   base64.b32encode(s).decode('ascii') +
                   "'))()")
        assert not subprocess.call([sys.executable, '-c', command])

    def test_import(self):
        # like test_multiprocess except subpackage modules referenced directly
        # (unlike test_submodule)
        global etree
        def scope():
            import xml.etree as foobar
            def example():
                x = etree.Comment
                x = foobar.ElementTree
            return example
        example = scope()
        import xml.etree.ElementTree as etree

        s = cloudpickle.dumps(example, protocol=self.protocol)

        command = ("import base64; "
                   "from srsly.cloudpickle.compat import pickle; "
                   "pickle.loads(base64.b32decode('" +
                   base64.b32encode(s).decode('ascii') +
                   "'))()")
        assert not subprocess.call([sys.executable, '-c', command])

    def test_multiprocessing_lock_raises(self):
        lock = multiprocessing.Lock()
        with pytest.raises(RuntimeError, match="only be shared between processes through inheritance"):
            cloudpickle.dumps(lock)

    def test_cell_manipulation(self):
        cell = _make_empty_cell()

        with pytest.raises(ValueError):
            cell.cell_contents

        ob = object()
        cell_set(cell, ob)
        self.assertTrue(
            cell.cell_contents is ob,
            msg='cell contents not set correctly',
        )

    def check_logger(self, name):
        logger = logging.getLogger(name)
        pickled = pickle_depickle(logger, protocol=self.protocol)
        self.assertTrue(pickled is logger, (pickled, logger))

        dumped = cloudpickle.dumps(logger)

        code = """if 1:
            import base64, srsly.cloudpickle as cloudpickle, logging

            logging.basicConfig(level=logging.INFO)
            logger = cloudpickle.loads(base64.b32decode(b'{}'))
            logger.info('hello')
            """.format(base64.b32encode(dumped).decode('ascii'))
        proc = subprocess.Popen([sys.executable, "-W ignore", "-c", code],
                                stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        out, _ = proc.communicate()
        self.assertEqual(proc.wait(), 0)
        self.assertEqual(out.strip().decode(),
                         f'INFO:{logger.name}:hello')

    def test_logger(self):
        # logging.RootLogger object
        self.check_logger(None)
        # logging.Logger object
        self.check_logger('cloudpickle.dummy_test_logger')

    def test_getset_descriptor(self):
        assert isinstance(float.real, types.GetSetDescriptorType)
        depickled_descriptor = pickle_depickle(float.real)
        self.assertIs(depickled_descriptor, float.real)

    def test_abc_cache_not_pickled(self):
        # cloudpickle issue #302: make sure that cloudpickle does not pickle
        # the caches populated during instance/subclass checks of abc.ABCMeta
        # instances.
        MyClass = abc.ABCMeta('MyClass', (), {})

        class MyUnrelatedClass:
            pass

        class MyRelatedClass:
            pass

        MyClass.register(MyRelatedClass)

        assert not issubclass(MyUnrelatedClass, MyClass)
        assert issubclass(MyRelatedClass, MyClass)

        s = cloudpickle.dumps(MyClass)

        assert b"MyUnrelatedClass" not in s
        assert b"MyRelatedClass" in s

        depickled_class = cloudpickle.loads(s)
        assert not issubclass(MyUnrelatedClass, depickled_class)
        assert issubclass(MyRelatedClass, depickled_class)

    def test_abc(self):

        class AbstractClass(abc.ABC):
            @abc.abstractmethod
            def some_method(self):
                """A method"""

            @classmethod
            @abc.abstractmethod
            def some_classmethod(cls):
                """A classmethod"""

            @staticmethod
            @abc.abstractmethod
            def some_staticmethod():
                """A staticmethod"""

            @property
            @abc.abstractmethod
            def some_property():
                """A property"""

        class ConcreteClass(AbstractClass):
            def some_method(self):
                return 'it works!'

            @classmethod
            def some_classmethod(cls):
                assert cls == ConcreteClass
                return 'it works!'

            @staticmethod
            def some_staticmethod():
                return 'it works!'

            @property
            def some_property(self):
                return 'it works!'

        # This abstract class is locally defined so we can safely register
        # tuple in it to verify the unpickled class also register tuple.
        AbstractClass.register(tuple)

        concrete_instance = ConcreteClass()
        depickled_base = pickle_depickle(AbstractClass, protocol=self.protocol)
        depickled_class = pickle_depickle(ConcreteClass,
                                          protocol=self.protocol)
        depickled_instance = pickle_depickle(concrete_instance)

        assert issubclass(tuple, AbstractClass)
        assert issubclass(tuple, depickled_base)

        self.assertEqual(depickled_class().some_method(), 'it works!')
        self.assertEqual(depickled_instance.some_method(), 'it works!')

        self.assertEqual(depickled_class.some_classmethod(), 'it works!')
        self.assertEqual(depickled_instance.some_classmethod(), 'it works!')

        self.assertEqual(depickled_class().some_staticmethod(), 'it works!')
        self.assertEqual(depickled_instance.some_staticmethod(), 'it works!')

        self.assertEqual(depickled_class().some_property, 'it works!')
        self.assertEqual(depickled_instance.some_property, 'it works!')
        self.assertRaises(TypeError, depickled_base)

        class DepickledBaseSubclass(depickled_base):
            def some_method(self):
                return 'it works for realz!'

            @classmethod
            def some_classmethod(cls):
                assert cls == DepickledBaseSubclass
                return 'it works for realz!'

            @staticmethod
            def some_staticmethod():
                return 'it works for realz!'

            @property
            def some_property():
                return 'it works for realz!'

        self.assertEqual(DepickledBaseSubclass().some_method(),
                         'it works for realz!')

        class IncompleteBaseSubclass(depickled_base):
            def some_method(self):
                return 'this class lacks some concrete methods'

        self.assertRaises(TypeError, IncompleteBaseSubclass)

    def test_abstracts(self):
        # Same as `test_abc` but using deprecated `abc.abstract*` methods.
        # See https://github.com/cloudpipe/cloudpickle/issues/367

        class AbstractClass(abc.ABC):
            @abc.abstractmethod
            def some_method(self):
                """A method"""

            @abc.abstractclassmethod
            def some_classmethod(cls):
                """A classmethod"""

            @abc.abstractstaticmethod
            def some_staticmethod():
                """A staticmethod"""

            @abc.abstractproperty
            def some_property(self):
                """A property"""

        class ConcreteClass(AbstractClass):
            def some_method(self):
                return 'it works!'

            @classmethod
            def some_classmethod(cls):
                assert cls == ConcreteClass
                return 'it works!'

            @staticmethod
            def some_staticmethod():
                return 'it works!'

            @property
            def some_property(self):
                return 'it works!'

        # This abstract class is locally defined so we can safely register
        # tuple in it to verify the unpickled class also register tuple.
        AbstractClass.register(tuple)

        concrete_instance = ConcreteClass()
        depickled_base = pickle_depickle(AbstractClass, protocol=self.protocol)
        depickled_class = pickle_depickle(ConcreteClass,
                                          protocol=self.protocol)
        depickled_instance = pickle_depickle(concrete_instance)

        assert issubclass(tuple, AbstractClass)
        assert issubclass(tuple, depickled_base)

        self.assertEqual(depickled_class().some_method(), 'it works!')
        self.assertEqual(depickled_instance.some_method(), 'it works!')

        self.assertEqual(depickled_class.some_classmethod(), 'it works!')
        self.assertEqual(depickled_instance.some_classmethod(), 'it works!')

        self.assertEqual(depickled_class().some_staticmethod(), 'it works!')
        self.assertEqual(depickled_instance.some_staticmethod(), 'it works!')

        self.assertEqual(depickled_class().some_property, 'it works!')
        self.assertEqual(depickled_instance.some_property, 'it works!')
        self.assertRaises(TypeError, depickled_base)

        class DepickledBaseSubclass(depickled_base):
            def some_method(self):
                return 'it works for realz!'

            @classmethod
            def some_classmethod(cls):
                assert cls == DepickledBaseSubclass
                return 'it works for realz!'

            @staticmethod
            def some_staticmethod():
                return 'it works for realz!'

            @property
            def some_property(self):
                return 'it works for realz!'

        self.assertEqual(DepickledBaseSubclass().some_method(),
                         'it works for realz!')

        class IncompleteBaseSubclass(depickled_base):
            def some_method(self):
                return 'this class lacks some concrete methods'

        self.assertRaises(TypeError, IncompleteBaseSubclass)

    def test_weakset_identity_preservation(self):
        # Test that weaksets don't lose all their inhabitants if they're
        # pickled in a larger data structure that includes other references to
        # their inhabitants.

        class SomeClass:
            def __init__(self, x):
                self.x = x

        obj1, obj2, obj3 = SomeClass(1), SomeClass(2), SomeClass(3)

        things = [weakref.WeakSet([obj1, obj2]), obj1, obj2, obj3]
        result = pickle_depickle(things, protocol=self.protocol)

        weakset, depickled1, depickled2, depickled3 = result

        self.assertEqual(depickled1.x, 1)
        self.assertEqual(depickled2.x, 2)
        self.assertEqual(depickled3.x, 3)
        self.assertEqual(len(weakset), 2)

        self.assertEqual(set(weakset), {depickled1, depickled2})

    def test_non_module_object_passing_whichmodule_test(self):
        # https://github.com/cloudpipe/cloudpickle/pull/326: cloudpickle should
        # not try to instrospect non-modules object when trying to discover the
        # module of a function/class. This happenened because codecov injects
        # tuples (and not modules) into sys.modules, but type-checks were not
        # carried out on the entries of sys.modules, causing cloupdickle to
        # then error in unexpected ways
        def func(x):
            return x ** 2

        # Trigger a loop during the execution of whichmodule(func) by
        # explicitly setting the function's module to None
        func.__module__ = None

        class NonModuleObject:
            def __ini__(self):
                self.some_attr = None

            def __getattr__(self, name):
                # We whitelist func so that a _whichmodule(func, None) call
                # returns the NonModuleObject instance if a type check on the
                # entries of sys.modules is not carried out, but manipulating
                # this instance thinking it really is a module later on in the
                # pickling process of func errors out
                if name == 'func':
                    return func
                else:
                    raise AttributeError

        non_module_object = NonModuleObject()

        assert func(2) == 4
        assert func is non_module_object.func

        # Any manipulation of non_module_object relying on attribute access
        # will raise an Exception
        with pytest.raises(AttributeError):
            _ = non_module_object.some_attr

        try:
            sys.modules['NonModuleObject'] = non_module_object

            func_module_name = _whichmodule(func, None)
            assert func_module_name != 'NonModuleObject'
            assert func_module_name is None

            depickled_func = pickle_depickle(func, protocol=self.protocol)
            assert depickled_func(2) == 4

        finally:
            sys.modules.pop('NonModuleObject')

    def test_unrelated_faulty_module(self):
        # Check that pickling a dynamically defined function or class does not
        # fail when introspecting the currently loaded modules in sys.modules
        # as long as those faulty modules are unrelated to the class or
        # function we are currently pickling.
        for base_class in (object, types.ModuleType):
            for module_name in ['_missing_module', None]:
                class FaultyModule(base_class):
                    def __getattr__(self, name):
                        # This throws an exception while looking up within
                        # pickle.whichmodule or getattr(module, name, None)
                        raise Exception()

                class Foo:
                    __module__ = module_name

                    def foo(self):
                        return "it works!"

                def foo():
                    return "it works!"

                foo.__module__ = module_name

                if base_class is types.ModuleType:  # noqa
                    faulty_module = FaultyModule('_faulty_module')
                else:
                    faulty_module = FaultyModule()
                sys.modules["_faulty_module"] = faulty_module

                try:
                    # Test whichmodule in save_global.
                    self.assertEqual(pickle_depickle(Foo()).foo(), "it works!")

                    # Test whichmodule in save_function.
                    cloned = pickle_depickle(foo, protocol=self.protocol)
                    self.assertEqual(cloned(), "it works!")
                finally:
                    sys.modules.pop("_faulty_module", None)

    @pytest.mark.skip(reason="fails for pytest v7.2.0")
    def test_dynamic_pytest_module(self):
        # Test case for pull request https://github.com/cloudpipe/cloudpickle/pull/116
        import py

        def f():
            s = py.builtin.set([1])
            return s.pop()

        # some setup is required to allow pytest apimodules to be correctly
        # serializable.
        from srsly.cloudpickle import CloudPickler
        from srsly.cloudpickle import cloudpickle_fast as cp_fast
        CloudPickler.dispatch_table[type(py.builtin)] = cp_fast._module_reduce

        g = cloudpickle.loads(cloudpickle.dumps(f, protocol=self.protocol))

        result = g()
        self.assertEqual(1, result)

    def test_function_module_name(self):
        func = lambda x: x
        cloned = pickle_depickle(func, protocol=self.protocol)
        self.assertEqual(cloned.__module__, func.__module__)

    def test_function_qualname(self):
        def func(x):
            return x
        # Default __qualname__ attribute (Python 3 only)
        if hasattr(func, '__qualname__'):
            cloned = pickle_depickle(func, protocol=self.protocol)
            self.assertEqual(cloned.__qualname__, func.__qualname__)

        # Mutated __qualname__ attribute
        func.__qualname__ = '<modifiedlambda>'
        cloned = pickle_depickle(func, protocol=self.protocol)
        self.assertEqual(cloned.__qualname__, func.__qualname__)

    def test_property(self):
        # Note that the @property decorator only has an effect on new-style
        # classes.
        class MyObject:
            _read_only_value = 1
            _read_write_value = 1

            @property
            def read_only_value(self):
                "A read-only attribute"
                return self._read_only_value

            @property
            def read_write_value(self):
                return self._read_write_value

            @read_write_value.setter
            def read_write_value(self, value):
                self._read_write_value = value



        my_object = MyObject()

        assert my_object.read_only_value == 1
        assert MyObject.read_only_value.__doc__ == "A read-only attribute"

        with pytest.raises(AttributeError):
            my_object.read_only_value = 2
        my_object.read_write_value = 2

        depickled_obj = pickle_depickle(my_object)

        assert depickled_obj.read_only_value == 1
        assert depickled_obj.read_write_value == 2

        # make sure the depickled read_only_value attribute is still read-only
        with pytest.raises(AttributeError):
            my_object.read_only_value = 2

        # make sure the depickled read_write_value attribute is writeable
        depickled_obj.read_write_value = 3
        assert depickled_obj.read_write_value == 3
        type(depickled_obj).read_only_value.__doc__ == "A read-only attribute"


    def test_namedtuple(self):
        MyTuple = collections.namedtuple('MyTuple', ['a', 'b', 'c'])
        t1 = MyTuple(1, 2, 3)
        t2 = MyTuple(3, 2, 1)

        depickled_t1, depickled_MyTuple, depickled_t2 = pickle_depickle(
            [t1, MyTuple, t2], protocol=self.protocol)

        assert isinstance(depickled_t1, MyTuple)
        assert depickled_t1 == t1
        assert depickled_MyTuple is MyTuple
        assert isinstance(depickled_t2, MyTuple)
        assert depickled_t2 == t2

    @pytest.mark.skipif(platform.python_implementation() == "PyPy",
        reason="fails sometimes for pypy on conda-forge")
    def test_interactively_defined_function(self):
        # Check that callables defined in the __main__ module of a Python
        # script (or jupyter kernel) can be pickled / unpickled / executed.
        code = """\
        from srsly.tests.cloudpickle.testutils import subprocess_pickle_echo

        CONSTANT = 42

        class Foo(object):

            def method(self, x):
                return x

        foo = Foo()

        def f0(x):
            return x ** 2

        def f1():
            return Foo

        def f2(x):
            return Foo().method(x)

        def f3():
            return Foo().method(CONSTANT)

        def f4(x):
            return foo.method(x)

        def f5(x):
            # Recursive call to a dynamically defined function.
            if x <= 0:
                return f4(x)
            return f5(x - 1) + 1

        cloned = subprocess_pickle_echo(lambda x: x**2, protocol={protocol})
        assert cloned(3) == 9

        cloned = subprocess_pickle_echo(f0, protocol={protocol})
        assert cloned(3) == 9

        cloned = subprocess_pickle_echo(Foo, protocol={protocol})
        assert cloned().method(2) == Foo().method(2)

        cloned = subprocess_pickle_echo(Foo(), protocol={protocol})
        assert cloned.method(2) == Foo().method(2)

        cloned = subprocess_pickle_echo(f1, protocol={protocol})
        assert cloned()().method('a') == f1()().method('a')

        cloned = subprocess_pickle_echo(f2, protocol={protocol})
        assert cloned(2) == f2(2)

        cloned = subprocess_pickle_echo(f3, protocol={protocol})
        assert cloned() == f3()

        cloned = subprocess_pickle_echo(f4, protocol={protocol})
        assert cloned(2) == f4(2)

        cloned = subprocess_pickle_echo(f5, protocol={protocol})
        assert cloned(7) == f5(7) == 7
        """.format(protocol=self.protocol)
        assert_run_python_script(textwrap.dedent(code))

    def test_interactively_defined_global_variable(self):
        # Check that callables defined in the __main__ module of a Python
        # script (or jupyter kernel) correctly retrieve global variables.
        code_template = """\
        from srsly.tests.cloudpickle.testutils import subprocess_pickle_echo
        from srsly.cloudpickle import dumps, loads

        def local_clone(obj, protocol=None):
            return loads(dumps(obj, protocol=protocol))

        VARIABLE = "default_value"

        def f0():
            global VARIABLE
            VARIABLE = "changed_by_f0"

        def f1():
            return VARIABLE

        assert f0.__globals__ is f1.__globals__

        # pickle f0 and f1 inside the same pickle_string
        cloned_f0, cloned_f1 = {clone_func}([f0, f1], protocol={protocol})

        # cloned_f0 and cloned_f1 now share a global namespace that is isolated
        # from any previously existing namespace
        assert cloned_f0.__globals__ is cloned_f1.__globals__
        assert cloned_f0.__globals__ is not f0.__globals__

        # pickle f1 another time, but in a new pickle string
        pickled_f1 = dumps(f1, protocol={protocol})

        # Change the value of the global variable in f0's new global namespace
        cloned_f0()

        # thanks to cloudpickle isolation, depickling and calling f0 and f1
        # should not affect the globals of already existing modules
        assert VARIABLE == "default_value", VARIABLE

        # Ensure that cloned_f1 and cloned_f0 share the same globals, as f1 and
        # f0 shared the same globals at pickling time, and cloned_f1 was
        # depickled from the same pickle string as cloned_f0
        shared_global_var = cloned_f1()
        assert shared_global_var == "changed_by_f0", shared_global_var

        # f1 is unpickled another time, but because it comes from another
        # pickle string than pickled_f1 and pickled_f0, it will not share the
        # same globals as the latter two.
        new_cloned_f1 = loads(pickled_f1)
        assert new_cloned_f1.__globals__ is not cloned_f1.__globals__
        assert new_cloned_f1.__globals__ is not f1.__globals__

        # get the value of new_cloned_f1's VARIABLE
        new_global_var = new_cloned_f1()
        assert new_global_var == "default_value", new_global_var
        """
        for clone_func in ['local_clone', 'subprocess_pickle_echo']:
            code = code_template.format(protocol=self.protocol,
                                        clone_func=clone_func)
            assert_run_python_script(textwrap.dedent(code))

    def test_closure_interacting_with_a_global_variable(self):
        global _TEST_GLOBAL_VARIABLE
        assert _TEST_GLOBAL_VARIABLE == "default_value"
        orig_value = _TEST_GLOBAL_VARIABLE
        try:
            def f0():
                global _TEST_GLOBAL_VARIABLE
                _TEST_GLOBAL_VARIABLE = "changed_by_f0"

            def f1():
                return _TEST_GLOBAL_VARIABLE

            # pickle f0 and f1 inside the same pickle_string
            cloned_f0, cloned_f1 = pickle_depickle([f0, f1],
                                                   protocol=self.protocol)

            # cloned_f0 and cloned_f1 now share a global namespace that is
            # isolated from any previously existing namespace
            assert cloned_f0.__globals__ is cloned_f1.__globals__
            assert cloned_f0.__globals__ is not f0.__globals__

            # pickle f1 another time, but in a new pickle string
            pickled_f1 = cloudpickle.dumps(f1, protocol=self.protocol)

            # Change the global variable's value in f0's new global namespace
            cloned_f0()

            # depickling f0 and f1 should not affect the globals of already
            # existing modules
            assert _TEST_GLOBAL_VARIABLE == "default_value"

            # Ensure that cloned_f1 and cloned_f0 share the same globals, as f1
            # and f0 shared the same globals at pickling time, and cloned_f1
            # was depickled from the same pickle string as cloned_f0
            shared_global_var = cloned_f1()
            assert shared_global_var == "changed_by_f0", shared_global_var

            # f1 is unpickled another time, but because it comes from another
            # pickle string than pickled_f1 and pickled_f0, it will not share
            # the same globals as the latter two.
            new_cloned_f1 = pickle.loads(pickled_f1)
            assert new_cloned_f1.__globals__ is not cloned_f1.__globals__
            assert new_cloned_f1.__globals__ is not f1.__globals__

            # get the value of new_cloned_f1's VARIABLE
            new_global_var = new_cloned_f1()
            assert new_global_var == "default_value", new_global_var
        finally:
            _TEST_GLOBAL_VARIABLE = orig_value

    def test_interactive_remote_function_calls(self):
        code = """if __name__ == "__main__":
        from srsly.tests.cloudpickle.testutils import subprocess_worker

        def interactive_function(x):
            return x + 1

        with subprocess_worker(protocol={protocol}) as w:

            assert w.run(interactive_function, 41) == 42

            # Define a new function that will call an updated version of
            # the previously called function:

            def wrapper_func(x):
                return interactive_function(x)

            def interactive_function(x):
                return x - 1

            # The change in the definition of interactive_function in the main
            # module of the main process should be reflected transparently
            # in the worker process: the worker process does not recall the
            # previous definition of `interactive_function`:

            assert w.run(wrapper_func, 41) == 40
        """.format(protocol=self.protocol)
        assert_run_python_script(code)

    def test_interactive_remote_function_calls_no_side_effect(self):
        code = """if __name__ == "__main__":
        from srsly.tests.cloudpickle.testutils import subprocess_worker
        import sys

        with subprocess_worker(protocol={protocol}) as w:

            GLOBAL_VARIABLE = 0

            class CustomClass(object):

                def mutate_globals(self):
                    global GLOBAL_VARIABLE
                    GLOBAL_VARIABLE += 1
                    return GLOBAL_VARIABLE

            custom_object = CustomClass()
            assert w.run(custom_object.mutate_globals) == 1

            # The caller global variable is unchanged in the main process.

            assert GLOBAL_VARIABLE == 0

            # Calling the same function again starts again from zero. The
            # worker process is stateless: it has no memory of the past call:

            assert w.run(custom_object.mutate_globals) == 1

            # The symbols defined in the main process __main__ module are
            # not set in the worker process main module to leave the worker
            # as stateless as possible:

            def is_in_main(name):
                return hasattr(sys.modules["__main__"], name)

            assert is_in_main("CustomClass")
            assert not w.run(is_in_main, "CustomClass")

            assert is_in_main("GLOBAL_VARIABLE")
            assert not w.run(is_in_main, "GLOBAL_VARIABLE")

        """.format(protocol=self.protocol)
        assert_run_python_script(code)

    def test_interactive_dynamic_type_and_remote_instances(self):
        code = """if __name__ == "__main__":
        from srsly.tests.cloudpickle.testutils import subprocess_worker

        with subprocess_worker(protocol={protocol}) as w:

            class CustomCounter:
                def __init__(self):
                    self.count = 0
                def increment(self):
                    self.count += 1
                    return self

            counter = CustomCounter().increment()
            assert counter.count == 1

            returned_counter = w.run(counter.increment)
            assert returned_counter.count == 2, returned_counter.count

            # Check that the class definition of the returned instance was
            # matched back to the original class definition living in __main__.

            assert isinstance(returned_counter, CustomCounter)

            # Check that memoization does not break provenance tracking:

            def echo(*args):
                return args

            C1, C2, c1, c2 = w.run(echo, CustomCounter, CustomCounter,
                                   CustomCounter(), returned_counter)
            assert C1 is CustomCounter
            assert C2 is CustomCounter
            assert isinstance(c1, CustomCounter)
            assert isinstance(c2, CustomCounter)

        """.format(protocol=self.protocol)
        assert_run_python_script(code)

    def test_interactive_dynamic_type_and_stored_remote_instances(self):
        """Simulate objects stored on workers to check isinstance semantics

        Such instances stored in the memory of running worker processes are
        similar to dask-distributed futures for instance.
        """
        code = """if __name__ == "__main__":
        import srsly.cloudpickle as cloudpickle, uuid
        from srsly.tests.cloudpickle.testutils import subprocess_worker

        with subprocess_worker(protocol={protocol}) as w:

            class A:
                '''Original class definition'''
                pass

            def store(x):
                storage = getattr(cloudpickle, "_test_storage", None)
                if storage is None:
                    storage = cloudpickle._test_storage = dict()
                obj_id = uuid.uuid4().hex
                storage[obj_id] = x
                return obj_id

            def lookup(obj_id):
                return cloudpickle._test_storage[obj_id]

            id1 = w.run(store, A())

            # The stored object on the worker is matched to a singleton class
            # definition thanks to provenance tracking:
            assert w.run(lambda obj_id: isinstance(lookup(obj_id), A), id1)

            # Retrieving the object from the worker yields a local copy that
            # is matched back the local class definition this instance
            # originally stems from.
            assert isinstance(w.run(lookup, id1), A)

            # Changing the local class definition should be taken into account
            # in all subsequent calls. In particular the old instances on the
            # worker do not map back to the new class definition, neither on
            # the worker itself, nor locally on the main program when the old
            # instance is retrieved:

            class A:
                '''Updated class definition'''
                pass

            assert not w.run(lambda obj_id: isinstance(lookup(obj_id), A), id1)
            retrieved1 = w.run(lookup, id1)
            assert not isinstance(retrieved1, A)
            assert retrieved1.__class__ is not A
            assert retrieved1.__class__.__doc__ == "Original class definition"

            # New instances on the other hand are proper instances of the new
            # class definition everywhere:

            a = A()
            id2 = w.run(store, a)
            assert w.run(lambda obj_id: isinstance(lookup(obj_id), A), id2)
            assert isinstance(w.run(lookup, id2), A)

            # Monkeypatch the class defintion in the main process to a new
            # class method:
            A.echo = lambda cls, x: x

            # Calling this method on an instance will automatically update
            # the remote class definition on the worker to propagate the monkey
            # patch dynamically.
            assert w.run(a.echo, 42) == 42

            # The stored instance can therefore also access the new class
            # method:
            assert w.run(lambda obj_id: lookup(obj_id).echo(43), id2) == 43

        """.format(protocol=self.protocol)
        assert_run_python_script(code)

    @pytest.mark.skip(reason="Seems to have issues outside of linux and CPython")
    def test_interactive_remote_function_calls_no_memory_leak(self):
        code = """if __name__ == "__main__":
        from srsly.tests.cloudpickle.testutils import subprocess_worker
        import struct

        with subprocess_worker(protocol={protocol}) as w:

            reference_size = w.memsize()
            assert reference_size > 0


            def make_big_closure(i):
                # Generate a byte string of size 1MB
                itemsize = len(struct.pack("l", 1))
                data = struct.pack("l", i) * (int(1e6) // itemsize)
                def process_data():
                    return len(data)
                return process_data

            for i in range(100):
                func = make_big_closure(i)
                result = w.run(func)
                assert result == int(1e6), result

            import gc
            w.run(gc.collect)

            # By this time the worker process has processed 100MB worth of data
            # passed in the closures. The worker memory size should not have
            # grown by more than a few MB as closures are garbage collected at
            # the end of each remote function call.
            growth = w.memsize() - reference_size

            # For some reason, the memory growth after processing 100MB of
            # data is ~10MB on MacOS, and ~1MB on Linux, so the upper bound on
            # memory growth we use is only tight for MacOS. However,
            # - 10MB is still 10x lower than the expected memory growth in case
            # of a leak (which would be the total size of the processed data,
            # 100MB)
            # - the memory usage growth does not increase if using 10000
            # iterations instead of 100 as used now (100x more data)
            assert growth < 1.5e7, growth

        """.format(protocol=self.protocol)
        assert_run_python_script(code)

    def test_pickle_reraise(self):
        for exc_type in [Exception, ValueError, TypeError, RuntimeError]:
            obj = RaiserOnPickle(exc_type("foo"))
            with pytest.raises((exc_type, pickle.PicklingError)):
                cloudpickle.dumps(obj, protocol=self.protocol)

    def test_unhashable_function(self):
        d = {'a': 1}
        depickled_method = pickle_depickle(d.get, protocol=self.protocol)
        self.assertEqual(depickled_method('a'), 1)
        self.assertEqual(depickled_method('b'), None)

    @pytest.mark.skipif(sys.version_info >= (3, 12), reason="Deprecation warning in python 3.12 about future deprecation in python 3.14")
    def test_itertools_count(self):
        counter = itertools.count(1, step=2)

        # advance the counter a bit
        next(counter)
        next(counter)

        new_counter = pickle_depickle(counter, protocol=self.protocol)

        self.assertTrue(counter is not new_counter)

        for _ in range(10):
            self.assertEqual(next(counter), next(new_counter))

    def test_wraps_preserves_function_name(self):
        from functools import wraps

        def f():
            pass

        @wraps(f)
        def g():
            f()

        f2 = pickle_depickle(g, protocol=self.protocol)

        self.assertEqual(f2.__name__, f.__name__)

    def test_wraps_preserves_function_doc(self):
        from functools import wraps

        def f():
            """42"""
            pass

        @wraps(f)
        def g():
            f()

        f2 = pickle_depickle(g, protocol=self.protocol)

        self.assertEqual(f2.__doc__, f.__doc__)

    def test_wraps_preserves_function_annotations(self):
        def f(x):
            pass

        f.__annotations__ = {'x': 1, 'return': float}

        @wraps(f)
        def g(x):
            f(x)

        f2 = pickle_depickle(g, protocol=self.protocol)

        self.assertEqual(f2.__annotations__, f.__annotations__)

    def test_type_hint(self):
        t = typing.Union[list, int]
        assert pickle_depickle(t) == t

    def test_instance_with_slots(self):
        for slots in [["registered_attribute"], "registered_attribute"]:
            class ClassWithSlots:
                __slots__ = slots

                def __init__(self):
                    self.registered_attribute = 42

            initial_obj = ClassWithSlots()
            depickled_obj = pickle_depickle(
                initial_obj, protocol=self.protocol)

            for obj in [initial_obj, depickled_obj]:
                self.assertEqual(obj.registered_attribute, 42)
                with pytest.raises(AttributeError):
                    obj.non_registered_attribute = 1

            class SubclassWithSlots(ClassWithSlots):
                def __init__(self):
                    self.unregistered_attribute = 1

            obj = SubclassWithSlots()
            s = cloudpickle.dumps(obj, protocol=self.protocol)
            del SubclassWithSlots
            depickled_obj = cloudpickle.loads(s)
            assert depickled_obj.unregistered_attribute == 1


    @unittest.skipIf(not hasattr(types, "MappingProxyType"),
                     "Old versions of Python do not have this type.")
    def test_mappingproxy(self):
        mp = types.MappingProxyType({"some_key": "some value"})
        assert mp == pickle_depickle(mp, protocol=self.protocol)

    def test_dataclass(self):
        dataclasses = pytest.importorskip("dataclasses")

        DataClass = dataclasses.make_dataclass('DataClass', [('x', int)])
        data = DataClass(x=42)

        pickle_depickle(DataClass, protocol=self.protocol)
        assert data.x == pickle_depickle(data, protocol=self.protocol).x == 42

    def test_locally_defined_enum(self):
        class StringEnum(str, enum.Enum):
            """Enum when all members are also (and must be) strings"""

        class Color(StringEnum):
            """3-element color space"""
            RED = "1"
            GREEN = "2"
            BLUE = "3"

            def is_green(self):
                return self is Color.GREEN

        green1, green2, ClonedColor = pickle_depickle(
            [Color.GREEN, Color.GREEN, Color], protocol=self.protocol)
        assert green1 is green2
        assert green1 is ClonedColor.GREEN
        assert green1 is not ClonedColor.BLUE
        assert isinstance(green1, str)
        assert green1.is_green()

        # cloudpickle systematically tracks provenance of class definitions
        # and ensure reconciliation in case of round trips:
        assert green1 is Color.GREEN
        assert ClonedColor is Color

        green3 = pickle_depickle(Color.GREEN, protocol=self.protocol)
        assert green3 is Color.GREEN

    def test_locally_defined_intenum(self):
        # Try again with a IntEnum defined with the functional API
        DynamicColor = enum.IntEnum("Color", {"RED": 1, "GREEN": 2, "BLUE": 3})

        green1, green2, ClonedDynamicColor = pickle_depickle(
            [DynamicColor.GREEN, DynamicColor.GREEN, DynamicColor],
            protocol=self.protocol)

        assert green1 is green2
        assert green1 is ClonedDynamicColor.GREEN
        assert green1 is not ClonedDynamicColor.BLUE
        assert ClonedDynamicColor is DynamicColor

    def test_interactively_defined_enum(self):
        code = """if __name__ == "__main__":
        from enum import Enum
        from srsly.tests.cloudpickle.testutils import subprocess_worker

        with subprocess_worker(protocol={protocol}) as w:

            class Color(Enum):
                RED = 1
                GREEN = 2

            def check_positive(x):
                return Color.GREEN if x >= 0 else Color.RED

            result = w.run(check_positive, 1)

            # Check that the returned enum instance is reconciled with the
            # locally defined Color enum type definition:

            assert result is Color.GREEN

            # Check that changing the definition of the Enum class is taken
            # into account on the worker for subsequent calls:

            class Color(Enum):
                RED = 1
                BLUE = 2

            def check_positive(x):
                return Color.BLUE if x >= 0 else Color.RED

            result = w.run(check_positive, 1)
            assert result is Color.BLUE
        """.format(protocol=self.protocol)
        assert_run_python_script(code)

    def test_relative_import_inside_function(self):
        pytest.importorskip("_cloudpickle_testpkg")
        # Make sure relative imports inside round-tripped functions is not
        # broken. This was a bug in cloudpickle versions <= 0.5.3 and was
        # re-introduced in 0.8.0.
        from _cloudpickle_testpkg import relative_imports_factory
        f, g = relative_imports_factory()
        for func, source in zip([f, g], ["module", "package"]):
            # Make sure relative imports are initially working
            assert func() == f"hello from a {source}!"

            # Make sure relative imports still work after round-tripping
            cloned_func = pickle_depickle(func, protocol=self.protocol)
            assert cloned_func() == f"hello from a {source}!"

    def test_interactively_defined_func_with_keyword_only_argument(self):
        # fixes https://github.com/cloudpipe/cloudpickle/issues/263
        def f(a, *, b=1):
            return a + b

        depickled_f = pickle_depickle(f, protocol=self.protocol)

        for func in (f, depickled_f):
            assert func(2) == 3
            assert func.__kwdefaults__ == {'b': 1}

    @pytest.mark.skipif(not hasattr(types.CodeType, "co_posonlyargcount"),
                        reason="Requires positional-only argument syntax")
    def test_interactively_defined_func_with_positional_only_argument(self):
        # Fixes https://github.com/cloudpipe/cloudpickle/issues/266
        # The source code of this test is bundled in a string and is ran from
        # the __main__ module of a subprocess in order to avoid a SyntaxError
        # in versions of python that do not support positional-only argument
        # syntax.
        code = """
        import pytest
        from srsly.cloudpickle import loads, dumps

        def f(a, /, b=1):
            return a + b

        depickled_f = loads(dumps(f, protocol={protocol}))

        for func in (f, depickled_f):
            assert func(2) == 3
            assert func.__code__.co_posonlyargcount == 1
            with pytest.raises(TypeError):
                func(a=2)

        """.format(protocol=self.protocol)
        assert_run_python_script(textwrap.dedent(code))

    def test___reduce___returns_string(self):
        # Non regression test for objects with a __reduce__ method returning a
        # string, meaning "save by attribute using save_global"
        pytest.importorskip("_cloudpickle_testpkg")
        from _cloudpickle_testpkg import some_singleton
        assert some_singleton.__reduce__() == "some_singleton"
        depickled_singleton = pickle_depickle(
            some_singleton, protocol=self.protocol)
        assert depickled_singleton is some_singleton

    def test_cloudpickle_extract_nested_globals(self):
        def function_factory():
            def inner_function():
                global _TEST_GLOBAL_VARIABLE
                return _TEST_GLOBAL_VARIABLE
            return inner_function

        globals_ = set(cloudpickle.cloudpickle._extract_code_globals(
            function_factory.__code__).keys())
        assert globals_ == {'_TEST_GLOBAL_VARIABLE'}

        depickled_factory = pickle_depickle(function_factory,
                                            protocol=self.protocol)
        inner_func = depickled_factory()
        assert inner_func() == _TEST_GLOBAL_VARIABLE

    def test_recursion_during_pickling(self):
        class A:
            def __getattribute__(self, name):
                return getattr(self, name)

        a = A()
        with pytest.raises(pickle.PicklingError, match='recursion'):
            cloudpickle.dumps(a)

    def test_out_of_band_buffers(self):
        if self.protocol < 5:
            pytest.skip("Need Pickle Protocol 5 or later")
        np = pytest.importorskip("numpy")

        class LocallyDefinedClass:
            data = np.zeros(10)

        data_instance = LocallyDefinedClass()
        buffers = []
        pickle_bytes = cloudpickle.dumps(data_instance, protocol=self.protocol,
                                         buffer_callback=buffers.append)
        assert len(buffers) == 1
        reconstructed = pickle.loads(pickle_bytes, buffers=buffers)
        np.testing.assert_allclose(reconstructed.data, data_instance.data)

    def test_pickle_dynamic_typevar(self):
        T = typing.TypeVar('T')
        depickled_T = pickle_depickle(T, protocol=self.protocol)
        attr_list = [
            "__name__", "__bound__", "__constraints__", "__covariant__",
            "__contravariant__"
        ]
        for attr in attr_list:
            assert getattr(T, attr) == getattr(depickled_T, attr)

    def test_pickle_dynamic_typevar_tracking(self):
        T = typing.TypeVar("T")
        T2 = subprocess_pickle_echo(T, protocol=self.protocol)
        assert T is T2

    def test_pickle_dynamic_typevar_memoization(self):
        T = typing.TypeVar('T')
        depickled_T1, depickled_T2 = pickle_depickle((T, T),
                                                     protocol=self.protocol)
        assert depickled_T1 is depickled_T2

    def test_pickle_importable_typevar(self):
        pytest.importorskip("_cloudpickle_testpkg")
        from _cloudpickle_testpkg import T
        T1 = pickle_depickle(T, protocol=self.protocol)
        assert T1 is T

        # Standard Library TypeVar
        from typing import AnyStr
        assert AnyStr is pickle_depickle(AnyStr, protocol=self.protocol)

    def test_generic_type(self):
        T = typing.TypeVar('T')

        class C(typing.Generic[T]):
            pass

        assert pickle_depickle(C, protocol=self.protocol) is C

        # Identity is not part of the typing contract: only test for
        # equality instead.
        assert pickle_depickle(C[int], protocol=self.protocol) == C[int]

        with subprocess_worker(protocol=self.protocol) as worker:

            def check_generic(generic, origin, type_value, use_args):
                assert generic.__origin__ is origin

                assert len(origin.__orig_bases__) == 1
                ob = origin.__orig_bases__[0]
                assert ob.__origin__ is typing.Generic

                if use_args:
                    assert len(generic.__args__) == 1
                    assert generic.__args__[0] is type_value
                else:
                    assert len(generic.__parameters__) == 1
                    assert generic.__parameters__[0] is type_value
                assert len(ob.__parameters__) == 1

                return "ok"

            # backward-compat for old Python 3.5 versions that sometimes relies
            # on __parameters__
            use_args = getattr(C[int], '__args__', ()) != ()
            assert check_generic(C[int], C, int, use_args) == "ok"
            assert worker.run(check_generic, C[int], C, int, use_args) == "ok"

    def test_generic_subclass(self):
        T = typing.TypeVar('T')

        class Base(typing.Generic[T]):
            pass

        class DerivedAny(Base):
            pass

        class LeafAny(DerivedAny):
            pass

        class DerivedInt(Base[int]):
            pass

        class LeafInt(DerivedInt):
            pass

        class DerivedT(Base[T]):
            pass

        class LeafT(DerivedT[T]):
            pass

        klasses = [
            Base, DerivedAny, LeafAny, DerivedInt, LeafInt, DerivedT, LeafT
        ]
        for klass in klasses:
            assert pickle_depickle(klass, protocol=self.protocol) is klass

        with subprocess_worker(protocol=self.protocol) as worker:

            def check_mro(klass, expected_mro):
                assert klass.mro() == expected_mro
                return "ok"

            for klass in klasses:
                mro = klass.mro()
                assert check_mro(klass, mro)
                assert worker.run(check_mro, klass, mro) == "ok"

    def test_locally_defined_class_with_type_hints(self):
        with subprocess_worker(protocol=self.protocol) as worker:
            for type_ in _all_types_to_test():
                class MyClass:
                    def method(self, arg: type_) -> type_:
                        return arg
                MyClass.__annotations__ = {'attribute': type_}

                def check_annotations(obj, expected_type, expected_type_str):
                    assert obj.__annotations__["attribute"] == expected_type
                    assert (
                        obj.method.__annotations__["arg"] == expected_type
                    )
                    assert (
                        obj.method.__annotations__["return"]
                        == expected_type
                    )
                    return "ok"

                obj = MyClass()
                assert check_annotations(obj, type_, "type_") == "ok"
                assert (
                    worker.run(check_annotations, obj, type_, "type_") == "ok"
                )

    def test_generic_extensions_literal(self):
        typing_extensions = pytest.importorskip('typing_extensions')
        for obj in [typing_extensions.Literal, typing_extensions.Literal['a']]:
            depickled_obj = pickle_depickle(obj, protocol=self.protocol)
            assert depickled_obj == obj

    def test_generic_extensions_final(self):
        typing_extensions = pytest.importorskip('typing_extensions')
        for obj in [typing_extensions.Final, typing_extensions.Final[int]]:
            depickled_obj = pickle_depickle(obj, protocol=self.protocol)
            assert depickled_obj == obj

    def test_class_annotations(self):
        class C:
            pass
        C.__annotations__ = {'a': int}

        C1 = pickle_depickle(C, protocol=self.protocol)
        assert C1.__annotations__ == C.__annotations__

    def test_function_annotations(self):
        def f(a: int) -> str:
            pass

        f1 = pickle_depickle(f, protocol=self.protocol)
        assert f1.__annotations__ == f.__annotations__

    def test_always_use_up_to_date_copyreg(self):
        # test that updates of copyreg.dispatch_table are taken in account by
        # cloudpickle
        import copyreg
        try:
            class MyClass:
                pass

            def reduce_myclass(x):
                return MyClass, (), {'custom_reduce': True}

            copyreg.dispatch_table[MyClass] = reduce_myclass
            my_obj = MyClass()
            depickled_myobj = pickle_depickle(my_obj, protocol=self.protocol)
            assert hasattr(depickled_myobj, 'custom_reduce')
        finally:
            copyreg.dispatch_table.pop(MyClass)

    def test_literal_misdetection(self):
        # see https://github.com/cloudpipe/cloudpickle/issues/403
        class MyClass:
            @property
            def __values__(self):
                return ()

        o = MyClass()
        pickle_depickle(o, protocol=self.protocol)

    def test_final_or_classvar_misdetection(self):
        # see https://github.com/cloudpipe/cloudpickle/issues/403
        class MyClass:
            @property
            def __type__(self):
                return int

        o = MyClass()
        pickle_depickle(o, protocol=self.protocol)

    @pytest.mark.skip(reason="Requires pytest -s to pass")
    def test_pickle_constructs_from_module_registered_for_pickling_by_value(self):  # noqa
        _prev_sys_path = sys.path.copy()
        try:
            # We simulate an interactive session that:
            # - we start from the /path/to/cloudpickle/tests directory, where a
            #   local .py file (mock_local_file) is located.
            # - uses constructs from mock_local_file in remote workers that do
            #   not have access to this file. This situation is
            #   the justification behind the
            #   (un)register_pickle_by_value(module) api that cloudpickle
            #   exposes.
            _mock_interactive_session_cwd = os.path.dirname(__file__)

            # First, remove sys.path entries that could point to
            # /path/to/cloudpickle/tests and be in inherited by the worker
            _maybe_remove(sys.path, '')
            _maybe_remove(sys.path, _mock_interactive_session_cwd)

            # Add the desired session working directory
            sys.path.insert(0, _mock_interactive_session_cwd)

            with subprocess_worker(protocol=self.protocol) as w:
                # Make the module unavailable in the remote worker
                w.run(
                    lambda p: sys.path.remove(p), _mock_interactive_session_cwd
                )
                # Import the actual file after starting the module since the
                # worker is started using fork on Linux, which will inherits
                # the parent sys.modules. On Python>3.6, the worker can be
                # started using spawn using mp_context in ProcessPoolExectutor.
                # TODO Once Python 3.6 reaches end of life, rely on mp_context
                # instead.
                import mock_local_folder.mod as mod
                # The constructs whose pickling mechanism is changed using
                # register_pickle_by_value are functions, classes, TypeVar and
                # modules.
                from mock_local_folder.mod import (
                    local_function, LocalT, LocalClass
                )

                # Make sure the module/constructs are unimportable in the
                # worker.
                with pytest.raises(ImportError):
                    w.run(lambda: __import__("mock_local_folder.mod"))
                with pytest.raises(ImportError):
                    w.run(
                        lambda: __import__("mock_local_folder.subfolder.mod")
                    )

                for o in [mod, local_function, LocalT, LocalClass]:
                    with pytest.raises(ImportError):
                        w.run(lambda: o)

                register_pickle_by_value(mod)
                # function
                assert w.run(lambda: local_function()) == local_function()
                # typevar
                assert w.run(lambda: LocalT.__name__) == LocalT.__name__
                # classes
                assert (
                    w.run(lambda: LocalClass().method())
                    == LocalClass().method()
                )
                # modules
                assert (
                    w.run(lambda: mod.local_function()) == local_function()
                )

                # Constructs from modules inside subfolders should be pickled
                # by value if a namespace module pointing to some parent folder
                # was registered for pickling by value. A "mock_local_folder"
                # namespace module falls into that category, but a
                # "mock_local_folder.mod" one does not.
                from mock_local_folder.subfolder.submod import (
                    LocalSubmodClass, LocalSubmodT, local_submod_function
                )
                # Shorter aliases to comply with line-length limits
                _t, _func, _class = (
                    LocalSubmodT, local_submod_function, LocalSubmodClass
                )
                with pytest.raises(ImportError):
                    w.run(
                        lambda: __import__("mock_local_folder.subfolder.mod")
                    )
                with pytest.raises(ImportError):
                    w.run(lambda: local_submod_function)

                unregister_pickle_by_value(mod)

                with pytest.raises(ImportError):
                    w.run(lambda: local_function)

                with pytest.raises(ImportError):
                    w.run(lambda: __import__("mock_local_folder.mod"))

                # Test the namespace folder case
                import mock_local_folder
                register_pickle_by_value(mock_local_folder)
                assert w.run(lambda: local_function()) == local_function()
                assert w.run(lambda: _func()) == _func()
                unregister_pickle_by_value(mock_local_folder)

                with pytest.raises(ImportError):
                    w.run(lambda: local_function)
                with pytest.raises(ImportError):
                    w.run(lambda: local_submod_function)

                # Test the case of registering a single module inside a
                # subfolder.
                import mock_local_folder.subfolder.submod
                register_pickle_by_value(mock_local_folder.subfolder.submod)
                assert w.run(lambda: _func()) == _func()
                assert w.run(lambda: _t.__name__) == _t.__name__
                assert w.run(lambda: _class().method()) == _class().method()

                # Registering a module from a subfolder for pickling by value
                # should not make constructs from modules from the parent
                # folder pickleable
                with pytest.raises(ImportError):
                    w.run(lambda: local_function)
                with pytest.raises(ImportError):
                    w.run(lambda: __import__("mock_local_folder.mod"))

                unregister_pickle_by_value(
                    mock_local_folder.subfolder.submod
                )
                with pytest.raises(ImportError):
                    w.run(lambda: local_submod_function)

                # Test the subfolder namespace module case
                import mock_local_folder.subfolder
                register_pickle_by_value(mock_local_folder.subfolder)
                assert w.run(lambda: _func()) == _func()
                assert w.run(lambda: _t.__name__) == _t.__name__
                assert w.run(lambda: _class().method()) == _class().method()

                unregister_pickle_by_value(mock_local_folder.subfolder)
        finally:
            _fname = "mock_local_folder"
            sys.path = _prev_sys_path
            for m in [_fname, f"{_fname}.mod", f"{_fname}.subfolder",
                      f"{_fname}.subfolder.submod"]:
                mod = sys.modules.pop(m, None)
                if mod and mod.__name__ in list_registry_pickle_by_value():
                    unregister_pickle_by_value(mod)

    def test_pickle_constructs_from_installed_packages_registered_for_pickling_by_value(  # noqa
        self
    ):
        pytest.importorskip("_cloudpickle_testpkg")
        for package_or_module in ["package", "module"]:
            if package_or_module == "package":
                import _cloudpickle_testpkg as m
                f = m.package_function_with_global
                _original_global = m.global_variable
            elif package_or_module == "module":
                import _cloudpickle_testpkg.mod as m
                f = m.module_function_with_global
                _original_global = m.global_variable
            try:
                with subprocess_worker(protocol=self.protocol) as w:
                    assert w.run(lambda: f()) == _original_global

                    # Test that f is pickled by value by modifying a global
                    # variable that f uses, and making sure that this
                    # modification shows up when calling the function remotely
                    register_pickle_by_value(m)
                    assert w.run(lambda: f()) == _original_global
                    m.global_variable = "modified global"
                    assert m.global_variable != _original_global
                    assert w.run(lambda: f()) == "modified global"
                    unregister_pickle_by_value(m)
            finally:
                m.global_variable = _original_global
                if m.__name__ in list_registry_pickle_by_value():
                    unregister_pickle_by_value(m)

    def test_pickle_various_versions_of_the_same_function_with_different_pickling_method(  # noqa
        self
    ):
        pytest.importorskip("_cloudpickle_testpkg")
        # Make sure that different versions of the same function (possibly
        # pickled in a different way - by value and/or by reference) can
        # peacefully co-exist (e.g. without globals interaction) in a remote
        # worker.
        import _cloudpickle_testpkg
        from _cloudpickle_testpkg import package_function_with_global as f
        _original_global = _cloudpickle_testpkg.global_variable

        def _create_registry():
            _main = __import__("sys").modules["__main__"]
            _main._cloudpickle_registry = {}
            # global _cloudpickle_registry

        def _add_to_registry(v, k):
            _main = __import__("sys").modules["__main__"]
            _main._cloudpickle_registry[k] = v

        def _call_from_registry(k):
            _main = __import__("sys").modules["__main__"]
            return _main._cloudpickle_registry[k]()

        try:
            with subprocess_worker(protocol=self.protocol) as w:
                w.run(_create_registry)
                w.run(_add_to_registry, f, "f_by_ref")

                register_pickle_by_value(_cloudpickle_testpkg)
                _cloudpickle_testpkg.global_variable = "modified global"
                w.run(_add_to_registry, f, "f_by_val")
                assert (
                    w.run(_call_from_registry, "f_by_ref") == _original_global
                )
                assert (
                    w.run(_call_from_registry, "f_by_val") == "modified global"
                )

        finally:
            _cloudpickle_testpkg.global_variable = _original_global

            if "_cloudpickle_testpkg" in list_registry_pickle_by_value():
                unregister_pickle_by_value(_cloudpickle_testpkg)

    @pytest.mark.skipif(
        sys.version_info < (3, 7),
        reason="Determinism can only be guaranteed for Python 3.7+"
    )
    def test_deterministic_pickle_bytes_for_function(self):
        # Ensure that functions with references to several global names are
        # pickled to fixed bytes that do not depend on the PYTHONHASHSEED of
        # the Python process.
        vals = set()

        def func_with_globals():
            return _TEST_GLOBAL_VARIABLE + _TEST_GLOBAL_VARIABLE2

        for i in range(5):
            vals.add(
                subprocess_pickle_string(func_with_globals,
                                         protocol=self.protocol,
                                         add_env={"PYTHONHASHSEED": str(i)}))
        if len(vals) > 1:
            # Print additional debug info on stdout with dis:
            for val in vals:
                pickletools.dis(val)
            pytest.fail(
                "Expected a single deterministic payload, got %d/5" % len(vals)
            )


class Protocol2CloudPickleTest(CloudPickleTest):

    protocol = 2


def test_lookup_module_and_qualname_dynamic_typevar():
    T = typing.TypeVar('T')
    module_and_name = _lookup_module_and_qualname(T, name=T.__name__)
    assert module_and_name is None


def test_lookup_module_and_qualname_importable_typevar():
    pytest.importorskip("_cloudpickle_testpkg")
    import _cloudpickle_testpkg
    T = _cloudpickle_testpkg.T
    module_and_name = _lookup_module_and_qualname(T, name=T.__name__)
    assert module_and_name is not None
    module, name = module_and_name
    assert module is _cloudpickle_testpkg
    assert name == 'T'


def test_lookup_module_and_qualname_stdlib_typevar():
    module_and_name = _lookup_module_and_qualname(typing.AnyStr,
                                                  name=typing.AnyStr.__name__)
    assert module_and_name is not None
    module, name = module_and_name
    assert module is typing
    assert name == 'AnyStr'


def test_register_pickle_by_value():
    pytest.importorskip("_cloudpickle_testpkg")
    import _cloudpickle_testpkg as pkg
    import _cloudpickle_testpkg.mod as mod

    assert list_registry_pickle_by_value() == set()

    register_pickle_by_value(pkg)
    assert list_registry_pickle_by_value() == {pkg.__name__}

    register_pickle_by_value(mod)
    assert list_registry_pickle_by_value() == {pkg.__name__, mod.__name__}

    unregister_pickle_by_value(mod)
    assert list_registry_pickle_by_value() == {pkg.__name__}

    msg = f"Input should be a module object, got {pkg.__name__} instead"
    with pytest.raises(ValueError, match=msg):
        unregister_pickle_by_value(pkg.__name__)

    unregister_pickle_by_value(pkg)
    assert list_registry_pickle_by_value() == set()

    msg = f"{pkg} is not registered for pickle by value"
    with pytest.raises(ValueError, match=re.escape(msg)):
        unregister_pickle_by_value(pkg)

    msg = f"Input should be a module object, got {pkg.__name__} instead"
    with pytest.raises(ValueError, match=msg):
        register_pickle_by_value(pkg.__name__)

    dynamic_mod = types.ModuleType('dynamic_mod')
    msg = (
        f"{dynamic_mod} was not imported correctly, have you used an "
        f"`import` statement to access it?"
    )
    with pytest.raises(ValueError, match=re.escape(msg)):
        register_pickle_by_value(dynamic_mod)


def _all_types_to_test():
    T = typing.TypeVar('T')

    class C(typing.Generic[T]):
        pass

    types_to_test = [
        C, C[int],
        T, typing.Any, typing.Optional,
        typing.Generic, typing.Union,
        typing.Optional[int],
        typing.Generic[T],
        typing.Callable[[int], typing.Any],
        typing.Callable[..., typing.Any],
        typing.Callable[[], typing.Any],
        typing.Tuple[int, ...],
        typing.Tuple[int, C[int]],
        typing.List[int],
        typing.Dict[int, str],
        typing.ClassVar,
        typing.ClassVar[C[int]],
        typing.NoReturn,
    ]
    return types_to_test


def test_module_level_pickler():
    # #366: cloudpickle should expose its pickle.Pickler subclass as
    # cloudpickle.Pickler
    assert hasattr(cloudpickle, "Pickler")
    assert cloudpickle.Pickler is cloudpickle.CloudPickler


if __name__ == '__main__':
    unittest.main()
