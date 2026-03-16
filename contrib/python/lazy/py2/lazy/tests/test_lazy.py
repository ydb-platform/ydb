import sys
import functools
import inspect
import unittest

from lazy import lazy


class TestCase(unittest.TestCase):

    def assertException(self, exc_cls, pattern, func, *args, **kw):
        """Assert an exception of type 'exc_cls' is raised and
        'pattern' is contained in the exception message.
        """
        try:
            func(*args, **kw)
        except exc_cls as e:
            exc_str = str(e)
        else:
            self.fail('%s not raised' % (exc_cls.__name__,))

        if pattern not in exc_str:
            self.fail('%r not in %r' % (pattern, exc_str))


class LazyTests(TestCase):

    def test_evaluate(self):
        # Lazy attributes should be evaluated when accessed.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 1)

    def test_evaluate_once(self):
        # Lazy attributes should be evaluated only once.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(f.foo, 1)
        self.assertEqual(f.foo, 1)
        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 1)

    def test_private_attribute(self):
        # It should be possible to create private, name-mangled
        # lazy attributes.
        called = []

        class Foo(object):
            @lazy
            def __foo(self):
                called.append('foo')
                return 1
            def get_foo(self):
                return self.__foo

        f = Foo()
        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(len(called), 1)

    def test_reserved_attribute(self):
        # It should be possible to create reserved lazy attributes.
        called = []

        class Foo(object):
            @lazy
            def __foo__(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(f.__foo__, 1)
        self.assertEqual(f.__foo__, 1)
        self.assertEqual(f.__foo__, 1)
        self.assertEqual(len(called), 1)

    def test_result_shadows_descriptor(self):
        # The result of the function call should be stored in
        # the object __dict__, shadowing the descriptor.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertTrue(isinstance(Foo.foo, lazy))
        self.assertTrue(f.foo is f.foo)
        self.assertTrue(f.foo is f.__dict__['foo']) # !
        self.assertEqual(len(called), 1)

        self.assertEqual(f.foo, 1)
        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(f, 'foo')

        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 2)

        self.assertEqual(f.foo, 1)
        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 2)

    def test_readonly_object(self):
        # The descriptor should raise an AttributeError when lazy is
        # used on a read-only object (an object with __slots__).
        called = []

        class Foo(object):
            __slots__ = ()
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(len(called), 0)

        self.assertException(AttributeError,
            "'Foo' object has no attribute '__dict__'",
            getattr, f, 'foo')

        # The function was not called
        self.assertEqual(len(called), 0)

    def test_introspection(self):
        # The lazy decorator should support basic introspection.

        class Foo(object):
            def foo(self):
                """foo func doc"""
            @lazy
            def bar(self):
                """bar func doc"""

        self.assertEqual(Foo.foo.__name__, "foo")
        self.assertEqual(Foo.foo.__doc__, "foo func doc")
        self.assertEqual(Foo.foo.__module__, "__tests__.test_lazy")

        self.assertEqual(Foo.bar.__name__, "bar")
        self.assertEqual(Foo.bar.__doc__, "bar func doc")
        self.assertEqual(Foo.bar.__module__, "__tests__.test_lazy")

    def test_types(self):
        # A lazy attribute should be of type lazy.

        class Foo(object):
            @lazy
            def foo(self):
                return 1
            @property
            def bar(self):
                return "bar"

        self.assertEqual(type(Foo.foo), lazy)
        self.assertEqual(type(Foo.bar), property)

        f = Foo()
        self.assertEqual(type(f.foo), int)
        self.assertEqual(type(f.bar), str)

    def test_super(self):
        # A lazy attribute should work when invoked via super.

        class Foo(object):
            @lazy
            def foo(self):
                return 'foo'

        class Bar(Foo):
            @lazy
            def foo(self):
                return super(Bar, self).foo + 'x'

        class Baz(Foo):
            @lazy
            def foo(self):
                return super().foo + 'xx'

        b = Bar()
        self.assertEqual(b.foo, 'foox')
        self.assertEqual(b.foo, 'foox')

        if sys.version_info >= (3,):
            b = Baz()
            self.assertEqual(b.foo, 'fooxx')
            self.assertEqual(b.foo, 'fooxx')

    def test_super_binding(self):
        # It should be impossible to change the cache once set.

        class Foo(object):
            @lazy
            def foo(self):
                return 'foo'

        class Bar(Foo):
            @lazy
            def foo(self):
                return super(Bar, self).foo + 'x'

        b = Bar()
        self.assertEqual(b.foo, 'foox')

        orig_id = id(b.foo)
        self.assertEqual(b.foo, 'foox')
        self.assertEqual(orig_id, id(b.foo))

        self.assertEqual(super(Bar, b).foo, 'foox')
        self.assertEqual(orig_id, id(b.foo))

        lazy.invalidate(b, 'foo')
        self.assertEqual(super(Bar, b).foo, 'foo')
        self.assertEqual(b.foo, 'foo')

    def test_inherited_attribute(self):
        # Inherited attributes should be stored in the instance they are
        # called on.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 'foo'

        class Bar(Foo):
            pass

        b = Bar()
        self.assertFalse('foo' in b.__dict__)
        self.assertEqual(b.foo, 'foo')
        self.assertEqual(len(called), 1)
        self.assertEqual(b.foo, 'foo')
        self.assertEqual(len(called), 1)
        self.assertTrue('foo' in b.__dict__)

    def test_inherited_private_attribute(self):
        # Inherited private attributes should be stored in the instance
        # they are called on.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return self.__bar
            @lazy
            def __bar(self):
                called.append('bar')
                return 'bar'

        class Bar(Foo):
            pass

        b = Bar()
        self.assertFalse('foo' in b.__dict__)
        self.assertEqual(b.foo, 'bar')
        self.assertEqual(len(called), 2)
        self.assertEqual(b.foo, 'bar')
        self.assertEqual(len(called), 2)
        self.assertTrue('foo' in b.__dict__)

        if sys.version_info >= (3,):
            self.assertTrue('_Foo__bar' in b.__dict__)
        else:
            self.assertTrue('_Bar__bar' in b.__dict__) # !

    def test_find_descriptors(self):
        # It should be possible to find all lazy attributes of an object.

        def other(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

        class Foo(object):
            @lazy
            def foo(self):
                return 'foo'
            @lazy
            def __bar(self):
                return 'bar'
            @lazy
            @other
            def baz(self):
                return 'baz'
            @other
            @lazy
            def quux(self):
                return 'quux'

        f = Foo()
        cls = f.__class__

        descriptors = []
        for name in cls.__dict__:
            if isinstance(getattr(cls, name), lazy):
                descriptors.append(name)

        self.assertEqual(sorted(descriptors), ['_Foo__bar', 'baz', 'foo'])

    def test_find_inherited_descriptors(self):
        # It should be possible to find inherited lazy attributes of
        # an object.

        def other(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

        class Foo(object):
            @lazy
            def foo(self):
                return 'foo'
            @lazy
            def __bar(self):
                return 'bar'
            @lazy
            @other
            def baz(self):
                return 'baz'
            @other
            @lazy
            def quux(self):
                return 'quux'

        class Bar(Foo):
            @lazy
            def __bar(self):
                return 'bar'

        b = Bar()

        descriptors = []
        for cls in inspect.getmro(b.__class__):
            for name in cls.__dict__:
                if isinstance(getattr(cls, name), lazy):
                    descriptors.append(name)

        self.assertEqual(sorted(descriptors), ['_Bar__bar', '_Foo__bar', 'baz', 'foo'])

    def test_other_decorators_must_use_functools_wraps(self):
        # Other decorators may be combined with lazy if
        # a) lazy is the first (outermost) decorator, and
        # b) the decorators properly use functools.wraps.
        called = []

        def other(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

        class Foo(object):
            @lazy
            @other
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertTrue(isinstance(Foo.foo, lazy))
        self.assertTrue(f.foo is f.foo)
        self.assertTrue(f.foo is f.__dict__['foo'])
        self.assertEqual(len(called), 1)

    def test_lazy_decorator_must_come_first(self):
        # Things break when lazy is not the outermost decorator.
        called = []

        def other(func):
            @functools.wraps(func)
            def wrapper(*args, **kwargs):
                return func(*args, **kwargs)
            return wrapper

        class Foo(object):
            @other
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertFalse(isinstance(Foo.foo, lazy))
        self.assertNotEqual(f.foo, 1)
        self.assertException(TypeError,
            "'lazy' object is not callable",
            f.foo)

    def test_set_name(self):
        # In Python >= 3.6 __set_name__ is called on lazy attributes.
        called = []

        class Foo(object):
            def _foo(self):
                called.append('foo')
                return 1
            foo = lazy(_foo)

        f = Foo()
        self.assertTrue(isinstance(Foo.foo, lazy))
        self.assertTrue(f.foo is f.foo)

        if sys.version_info >= (3, 6):
            self.assertEqual(Foo.foo.__name__, 'foo')
            self.assertTrue(f.foo is f.__dict__['foo'])
        else:
            self.assertEqual(Foo.foo.__name__, '_foo')
            self.assertTrue(f.foo is f.__dict__['_foo'])

        self.assertEqual(len(called), 1)


class InvalidateTests(TestCase):

    def test_invalidate_attribute(self):
        # It should be possible to invalidate a lazy attribute.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(f, 'foo')

        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_attribute_twice(self):
        # It should be possible to invalidate a lazy attribute
        # twice without causing harm.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(f, 'foo')
        lazy.invalidate(f, 'foo') # Nothing happens

        self.assertEqual(f.foo, 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_uncalled_attribute(self):
        # It should be possible to invalidate an empty attribute
        # cache without causing harm.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(len(called), 0)
        lazy.invalidate(f, 'foo') # Nothing happens

    def test_invalidate_private_attribute(self):
        # It should be possible to invalidate a private lazy attribute.
        called = []

        class Foo(object):
            @lazy
            def __foo(self):
                called.append('foo')
                return 1
            def get_foo(self):
                return self.__foo

        f = Foo()
        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(f, '__foo')

        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_mangled_attribute(self):
        # It should be possible to invalidate a private lazy attribute
        # by its mangled name.
        called = []

        class Foo(object):
            @lazy
            def __foo(self):
                called.append('foo')
                return 1
            def get_foo(self):
                return self.__foo

        f = Foo()
        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(f, '_Foo__foo')

        self.assertEqual(f.get_foo(), 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_reserved_attribute(self):
        # It should be possible to invalidate a reserved lazy attribute.
        called = []

        class Foo(object):
            @lazy
            def __foo__(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertEqual(f.__foo__, 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(f, '__foo__')

        self.assertEqual(f.__foo__, 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_nonlazy_attribute(self):
        # Invalidating an attribute that is not lazy should
        # raise an AttributeError.
        called = []

        class Foo(object):
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertException(AttributeError,
            "'Foo.foo' is not a lazy attribute",
            lazy.invalidate, f, 'foo')

    def test_invalidate_nonlazy_private_attribute(self):
        # Invalidating a private attribute that is not lazy should
        # raise an AttributeError.
        called = []

        class Foo(object):
            def __foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertException(AttributeError,
            "'Foo._Foo__foo' is not a lazy attribute",
            lazy.invalidate, f, '__foo')

    def test_invalidate_unknown_attribute(self):
        # Invalidating an unknown attribute should
        # raise an AttributeError.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertException(AttributeError,
            "type object 'Foo' has no attribute 'bar'",
            lazy.invalidate, f, 'bar')

    def test_invalidate_readonly_object(self):
        # Calling invalidate on a read-only object should
        # raise an AttributeError.
        called = []

        class Foo(object):
            __slots__ = ()
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        f = Foo()
        self.assertException(AttributeError,
            "'Foo' object has no attribute '__dict__'",
            lazy.invalidate, f, 'foo')

    def test_invalidate_inherited_attribute(self):
        # It should be possible to invalidate an inherited lazy
        # attribute.
        called = []

        class Foo(object):
            @lazy
            def foo(self):
                called.append('foo')
                return 1

        class Bar(Foo):
            pass

        b = Bar()
        self.assertEqual(b.foo, 1)
        self.assertEqual(len(called), 1)
        self.assertEqual(b.foo, 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(b, 'foo')

        self.assertEqual(b.foo, 1)
        self.assertEqual(len(called), 2)


# A lazy subclass
class cached(lazy):
    pass


class InvalidateSubclassTests(TestCase):

    def test_invalidate_attribute(self):
        # It should be possible to invalidate a cached attribute.
        called = []

        class Bar(object):
            @cached
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertEqual(b.bar, 1)
        self.assertEqual(len(called), 1)

        cached.invalidate(b, 'bar')

        self.assertEqual(b.bar, 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_attribute_twice(self):
        # It should be possible to invalidate a cached attribute
        # twice without causing harm.
        called = []

        class Bar(object):
            @cached
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertEqual(b.bar, 1)
        self.assertEqual(len(called), 1)

        cached.invalidate(b, 'bar')
        cached.invalidate(b, 'bar') # Nothing happens

        self.assertEqual(b.bar, 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_uncalled_attribute(self):
        # It should be possible to invalidate an empty attribute
        # cache without causing harm.
        called = []

        class Bar(object):
            @cached
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertEqual(len(called), 0)
        cached.invalidate(b, 'bar') # Nothing happens

    def test_invalidate_private_attribute(self):
        # It should be possible to invalidate a private cached attribute.
        called = []

        class Bar(object):
            @cached
            def __bar(self):
                called.append('bar')
                return 1
            def get_bar(self):
                return self.__bar

        b = Bar()
        self.assertEqual(b.get_bar(), 1)
        self.assertEqual(len(called), 1)

        cached.invalidate(b, '__bar')

        self.assertEqual(b.get_bar(), 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_mangled_attribute(self):
        # It should be possible to invalidate a private cached attribute
        # by its mangled name.
        called = []

        class Bar(object):
            @cached
            def __bar(self):
                called.append('bar')
                return 1
            def get_bar(self):
                return self.__bar

        b = Bar()
        self.assertEqual(b.get_bar(), 1)
        self.assertEqual(len(called), 1)

        cached.invalidate(b, '_Bar__bar')

        self.assertEqual(b.get_bar(), 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_reserved_attribute(self):
        # It should be possible to invalidate a reserved cached attribute.
        called = []

        class Bar(object):
            @cached
            def __bar__(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertEqual(b.__bar__, 1)
        self.assertEqual(len(called), 1)

        cached.invalidate(b, '__bar__')

        self.assertEqual(b.__bar__, 1)
        self.assertEqual(len(called), 2)

    def test_invalidate_uncached_attribute(self):
        # Invalidating an attribute that is not cached should
        # raise an AttributeError.
        called = []

        class Bar(object):
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertException(AttributeError,
            "'Bar.bar' is not a cached attribute",
            cached.invalidate, b, 'bar')

    def test_invalidate_uncached_private_attribute(self):
        # Invalidating a private attribute that is not cached should
        # raise an AttributeError.
        called = []

        class Bar(object):
            def __bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertException(AttributeError,
            "'Bar._Bar__bar' is not a cached attribute",
            cached.invalidate, b, '__bar')

    def test_invalidate_unknown_attribute(self):
        # Invalidating an unknown attribute should
        # raise an AttributeError.
        called = []

        class Bar(object):
            @cached
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertException(AttributeError,
            "type object 'Bar' has no attribute 'baz'",
            lazy.invalidate, b, 'baz')

    def test_invalidate_readonly_object(self):
        # Calling invalidate on a read-only object should
        # raise an AttributeError.
        called = []

        class Bar(object):
            __slots__ = ()
            @cached
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertException(AttributeError,
            "'Bar' object has no attribute '__dict__'",
            cached.invalidate, b, 'bar')

    def test_invalidate_superclass_attribute(self):
        # cached.invalidate CANNOT invalidate a superclass (lazy) attribute.
        called = []

        class Bar(object):
            @lazy
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertException(AttributeError,
            "'Bar.bar' is not a cached attribute",
            cached.invalidate, b, 'bar')

    def test_invalidate_subclass_attribute(self):
        # Whereas lazy.invalidate CAN invalidate a subclass (cached) attribute.
        called = []

        class Bar(object):
            @cached
            def bar(self):
                called.append('bar')
                return 1

        b = Bar()
        self.assertEqual(b.bar, 1)
        self.assertEqual(len(called), 1)

        lazy.invalidate(b, 'bar')

        self.assertEqual(b.bar, 1)
        self.assertEqual(len(called), 2)


class AssertExceptionTests(TestCase):

    def test_assert_AttributeError(self):
        self.assertException(AttributeError,
            "'AssertExceptionTests' object has no attribute 'foobar'",
            getattr, self, 'foobar')

    def test_assert_IOError(self):
        self.assertException(IOError,
            "No such file or directory",
            open, './foo/bar/baz/peng/quux', 'rb')

    def test_assert_SystemExit(self):
        self.assertException(SystemExit,
            "",
            sys.exit)

    def test_assert_exception_not_raised(self):
        self.assertRaises(AssertionError,
            self.assertException, AttributeError,
            "'AssertExceptionTests' object has no attribute 'run'",
            getattr, self, 'run')

    def test_assert_pattern_mismatch(self):
        self.assertRaises(AssertionError,
            self.assertException, AttributeError,
            "baz",
            getattr, self, 'foobar')

