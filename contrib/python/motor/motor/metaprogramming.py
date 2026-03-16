# Copyright 2014 MongoDB, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Dynamic class-creation for Motor."""

import inspect
import functools

from pymongo.cursor import Cursor

_class_cache = {}


def asynchronize(
        framework, sync_method, doc=None, wrap_class=None, unwrap_class=None):
    """Decorate `sync_method` so it returns a Future.

    The method runs on a thread and resolves the Future when it completes.

    :Parameters:
     - `motor_class`:       Motor class being created, e.g. MotorClient.
     - `framework`:         An asynchronous framework
     - `sync_method`:       Unbound method of pymongo Collection, Database,
                            MongoClient, etc.
     - `doc`:               Optionally override sync_method's docstring
     - `wrap_class`:        Optional PyMongo class, wrap a returned object of
                            this PyMongo class in the equivalent Motor class
     - `unwrap_class`       Optional Motor class name, unwrap an argument with
                            this Motor class name and pass the wrapped PyMongo
                            object instead
    """
    @functools.wraps(sync_method)
    def method(self, *args, **kwargs):
        if unwrap_class is not None:
            # Don't call isinstance(), not checking subclasses.
            unwrapped_args = [
                obj.delegate
                if obj.__class__.__name__.endswith(
                    (unwrap_class, 'MotorClientSession'))
                else obj
                for obj in args]
            unwrapped_kwargs = {
                key: (obj.delegate
                      if obj.__class__.__name__.endswith(
                        (unwrap_class, 'MotorClientSession'))
                      else obj)
                for key, obj in kwargs.items()}
        else:
            # For speed, don't call unwrap_args_session/unwrap_kwargs_session.
            unwrapped_args = [
                obj.delegate
                if obj.__class__.__name__.endswith('MotorClientSession')
                else obj
                for obj in args]
            unwrapped_kwargs = {
                key: (obj.delegate
                      if obj.__class__.__name__.endswith('MotorClientSession')
                      else obj)
                for key, obj in kwargs.items()}

        loop = self.get_io_loop()
        return framework.run_on_executor(loop,
                                         sync_method,
                                         self.delegate,
                                         *unwrapped_args,
                                         **unwrapped_kwargs)

    if wrap_class is not None:
        method = framework.pymongo_class_wrapper(method, wrap_class)
        method.is_wrap_method = True  # For Synchro.

    # This is for the benefit of motor_extensions.py, which needs this info to
    # generate documentation with Sphinx.
    method.is_async_method = True
    name = sync_method.__name__
    method.pymongo_method_name = name

    if doc is not None:
        method.__doc__ = doc

    return method


def unwrap_args_session(args):
    return (
        obj.delegate
        if obj.__class__.__name__.endswith('MotorClientSession')
        else obj
        for obj in args)


def unwrap_kwargs_session(kwargs):
    return {
        key: (obj.delegate
              if obj.__class__.__name__.endswith('MotorClientSession')
              else obj)
        for key, obj in kwargs.items()}


_coro_token = object()


def coroutine_annotation(f):
    """In docs, annotate a function that returns a Future with 'coroutine'.

    This doesn't affect behavior.
    """
    # Like:
    # @coroutine_annotation
    # def method(self):
    #
    f.coroutine_annotation = True
    return f


class MotorAttributeFactory(object):
    """Used by Motor classes to mark attributes that delegate in some way to
    PyMongo. At module import time, create_class_with_framework calls
    create_attribute() for each attr to create the final class attribute.
    """
    def __init__(self, doc=None):
        self.doc = doc

    def create_attribute(self, cls, attr_name):
        raise NotImplementedError


class Async(MotorAttributeFactory):
    def __init__(self, attr_name, doc=None):
        """A descriptor that wraps a PyMongo method, such as insert_one,
        and returns an asynchronous version of the method that returns a Future.

        :Parameters:
         - `attr_name`: The name of the attribute on the PyMongo class, if
           different from attribute on the Motor class
        """
        super().__init__(doc)
        self.attr_name = attr_name
        self.wrap_class = None
        self.unwrap_class = None

    def create_attribute(self, cls, attr_name):
        name = self.attr_name or attr_name
        method = getattr(cls.__delegate_class__, name)
        return asynchronize(framework=cls._framework,
                            sync_method=method,
                            doc=self.doc,
                            wrap_class=self.wrap_class,
                            unwrap_class=self.unwrap_class)

    def wrap(self, original_class):
        self.wrap_class = original_class
        return self

    def unwrap(self, class_name):
        self.unwrap_class = class_name
        return self


class AsyncRead(Async):
    def __init__(self, attr_name=None, doc=None):
        """A descriptor that wraps a PyMongo read method like find_one() that
        returns a Future.
        """
        Async.__init__(self, attr_name=attr_name, doc=doc)


class AsyncWrite(Async):
    def __init__(self, attr_name=None, doc=None):
        """A descriptor that wraps a PyMongo write method like update() that
        accepts getLastError options and returns a Future.
        """
        Async.__init__(self, attr_name=attr_name, doc=doc)


class AsyncCommand(Async):
    def __init__(self, attr_name=None, doc=None):
        """A descriptor that wraps a PyMongo command like copy_database() that
        returns a Future and does not accept getLastError options.
        """
        Async.__init__(self, attr_name=attr_name, doc=doc)


class ReadOnlyProperty(MotorAttributeFactory):
    """Creates a readonly attribute on the wrapped PyMongo object."""
    def create_attribute(self, cls, attr_name):
        def fget(obj):
            return getattr(obj.delegate, attr_name)

        if self.doc:
            doc = self.doc
        else:
            doc = getattr(cls.__delegate_class__, attr_name).__doc__

        if doc:
            return property(fget=fget, doc=doc)
        else:
            return property(fget=fget)


class DelegateMethod(ReadOnlyProperty):
    """A method on the wrapped PyMongo object that does no I/O and can be called
    synchronously"""

    def __init__(self, doc=None):
        ReadOnlyProperty.__init__(self, doc)
        self.wrap_class = None

    def wrap(self, original_class):
        self.wrap_class = original_class
        return self

    def create_attribute(self, cls, attr_name):
        if self.wrap_class is None:
            return ReadOnlyProperty.create_attribute(self, cls, attr_name)

        method = getattr(cls.__delegate_class__, attr_name)
        original_class = self.wrap_class

        @functools.wraps(method)
        def wrapper(self_, *args, **kwargs):
            result = method(self_.delegate, *args, **kwargs)

            # Don't call isinstance(), not checking subclasses.
            if result.__class__ == original_class:
                # Delegate to the current object to wrap the result.
                return self_.wrap(result)
            else:
                return result

        if self.doc:
            wrapper.__doc__ = self.doc

        wrapper.is_wrap_method = True  # For Synchro.
        return wrapper


class MotorCursorChainingMethod(MotorAttributeFactory):
    def create_attribute(self, cls, attr_name):
        cursor_method = getattr(cls.__delegate_class__, attr_name)

        @functools.wraps(cursor_method)
        def return_clone(self, *args, **kwargs):
            cursor_method(self.delegate, *args, **kwargs)
            return self

        # This is for the benefit of Synchro, and motor_extensions.py
        return_clone.is_motorcursor_chaining_method = True
        return_clone.pymongo_method_name = attr_name
        if self.doc:
            return_clone.__doc__ = self.doc

        return return_clone


def create_class_with_framework(cls, framework, module_name):
    motor_class_name = framework.CLASS_PREFIX + cls.__motor_class_name__
    cache_key = (cls, motor_class_name, framework)
    cached_class = _class_cache.get(cache_key)
    if cached_class:
        return cached_class

    new_class = type(str(motor_class_name), (cls,), {})
    new_class.__module__ = module_name
    new_class._framework = framework

    assert hasattr(new_class, '__delegate_class__')

    # If we're constructing MotorClient from AgnosticClient, for example,
    # the method resolution order is (AgnosticClient, AgnosticBase, object).
    # Iterate over bases looking for attributes and coroutines that must be
    # replaced with framework-specific ones.
    for base in reversed(inspect.getmro(cls)):
        # Turn attribute factories into real methods or descriptors.
        for name, attr in base.__dict__.items():
            if isinstance(attr, MotorAttributeFactory):
                new_class_attr = attr.create_attribute(new_class, name)
                setattr(new_class, name, new_class_attr)

    _class_cache[cache_key] = new_class
    return new_class
