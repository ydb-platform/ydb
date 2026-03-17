# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.


"""Simple wrappers around Factory class definition."""

import contextlib
import logging

from . import base
from . import declarations


@contextlib.contextmanager
def debug(logger='factory', stream=None):
    logger_obj = logging.getLogger(logger)
    old_level = logger_obj.level

    handler = logging.StreamHandler(stream)
    handler.setLevel(logging.DEBUG)
    logger_obj.addHandler(handler)
    logger_obj.setLevel(logging.DEBUG)

    yield

    logger_obj.setLevel(old_level)
    logger_obj.removeHandler(handler)


def make_factory(klass, **kwargs):
    """Create a new, simple factory for the given class."""
    factory_name = '%sFactory' % klass.__name__

    class Meta:
        model = klass

    kwargs['Meta'] = Meta
    base_class = kwargs.pop('FACTORY_CLASS', base.Factory)

    factory_class = type(base.Factory).__new__(type(base.Factory), factory_name, (base_class,), kwargs)
    factory_class.__name__ = '%sFactory' % klass.__name__
    factory_class.__doc__ = 'Auto-generated factory for class %s' % klass
    return factory_class


def build(klass, **kwargs):
    """Create a factory for the given class, and build an instance."""
    return make_factory(klass, **kwargs).build()


def build_batch(klass, size, **kwargs):
    """Create a factory for the given class, and build a batch of instances."""
    return make_factory(klass, **kwargs).build_batch(size)


def create(klass, **kwargs):
    """Create a factory for the given class, and create an instance."""
    return make_factory(klass, **kwargs).create()


def create_batch(klass, size, **kwargs):
    """Create a factory for the given class, and create a batch of instances."""
    return make_factory(klass, **kwargs).create_batch(size)


def stub(klass, **kwargs):
    """Create a factory for the given class, and stub an instance."""
    return make_factory(klass, **kwargs).stub()


def stub_batch(klass, size, **kwargs):
    """Create a factory for the given class, and stub a batch of instances."""
    return make_factory(klass, **kwargs).stub_batch(size)


def generate(klass, strategy, **kwargs):
    """Create a factory for the given class, and generate an instance."""
    return make_factory(klass, **kwargs).generate(strategy)


def generate_batch(klass, strategy, size, **kwargs):
    """Create a factory for the given class, and generate instances."""
    return make_factory(klass, **kwargs).generate_batch(strategy, size)


def simple_generate(klass, create, **kwargs):
    """Create a factory for the given class, and simple_generate an instance."""
    return make_factory(klass, **kwargs).simple_generate(create)


def simple_generate_batch(klass, create, size, **kwargs):
    """Create a factory for the given class, and simple_generate instances."""
    return make_factory(klass, **kwargs).simple_generate_batch(create, size)


def lazy_attribute(func):
    return declarations.LazyAttribute(func)


def iterator(func):
    """Turn a generator function into an iterator attribute."""
    return declarations.Iterator(func())


def sequence(func):
    return declarations.Sequence(func)


def lazy_attribute_sequence(func):
    return declarations.LazyAttributeSequence(func)


def container_attribute(func):
    return declarations.ContainerAttribute(func, strict=False)


def post_generation(fun):
    return declarations.PostGeneration(fun)
