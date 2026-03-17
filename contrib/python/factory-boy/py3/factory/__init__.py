# -*- coding: utf-8 -*-
# Copyright: See the LICENSE file.

from .base import (
    Factory,
    BaseDictFactory,
    DictFactory,
    BaseListFactory,
    ListFactory,
    StubFactory,

    use_strategy,
)

from .enums import (
    BUILD_STRATEGY,
    CREATE_STRATEGY,
    STUB_STRATEGY,
)


from .errors import (
    FactoryError,
)

from .faker import Faker

from .declarations import (
    LazyFunction,
    LazyAttribute,
    Iterator,
    Sequence,
    LazyAttributeSequence,
    SelfAttribute,
    Trait,
    ContainerAttribute,
    SubFactory,
    Dict,
    List,
    Maybe,
    PostGeneration,
    PostGenerationMethodCall,
    RelatedFactory,
    RelatedFactoryList,
)

from .helpers import (
    debug,

    build,
    create,
    stub,
    generate,
    simple_generate,
    make_factory,

    build_batch,
    create_batch,
    stub_batch,
    generate_batch,
    simple_generate_batch,

    lazy_attribute,
    iterator,
    sequence,
    lazy_attribute_sequence,
    container_attribute,
    post_generation,
)

# Backward compatibility; this should be removed soon.
from . import alchemy
from . import django
from . import mogo
from . import mongoengine


__version__ = '2.12.0'
__author__ = 'Raphaël Barrois <raphael.barrois+fboy@polytechnique.org>'


MogoFactory = mogo.MogoFactory
DjangoModelFactory = django.DjangoModelFactory
