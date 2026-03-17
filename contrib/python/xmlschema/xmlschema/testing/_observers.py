#
# Copyright (c), 2016-2026, SISSA (International School for Advanced Studies).
# All rights reserved.
# This file is distributed under the terms of the MIT License.
# See the file 'LICENSE' in the root directory of the present
# distribution, or http://opensource.org/licenses/MIT.
#
# @author Davide Brunato <brunato@sissa.it>
#
# mypy: ignore-errors
"""
Observers for testing XMLSchema classes.
"""
from functools import wraps
from itertools import chain

from xmlschema.names import XSD_NAMESPACE, XSD_ANY_TYPE
from xmlschema.validators import XMLSchema10, XMLSchema11, XsdGroup, \
    XsdAttributeGroup, XsdComplexType, XsdComponent, XsdBuilders


class SchemaObserver:
    """
    Observer that registers created components. Run the 'clear' method after each usage.
    """
    components = []
    dummy_components = []

    @classmethod
    def observed_builder(cls, builder):
        if isinstance(builder, type):
            class BuilderProxy(builder):
                def __init__(self, *args, **kwargs):
                    super().__init__(*args, **kwargs)
                    assert isinstance(self, XsdComponent)

                    if not cls.is_dummy_component(self):
                        cls.components.append(self)
                    else:
                        cls.dummy_components.append(self)

            BuilderProxy.__name__ = builder.__name__
            return BuilderProxy

        elif callable(builder):
            @wraps(builder)
            def builder_proxy(*args, **kwargs):
                obj = builder(*args, **kwargs)
                assert isinstance(obj, XsdComponent)

                if not cls.is_dummy_component(obj):
                    cls.components.append(obj)
                else:
                    cls.dummy_components.append(obj)
                return obj

            return builder_proxy

    @classmethod
    def clear(cls) -> None:
        del cls.components[:]
        del cls.dummy_components[:]

    @classmethod
    def is_dummy_component(cls, component) -> bool:
        # Dummy components are empty attribute groups and xs:anyType
        # definitions not related to XSD namespace.
        if component.parent in cls.dummy_components:
            return True
        elif isinstance(component, XsdAttributeGroup):
            return not component
        elif isinstance(component, XsdComplexType):
            return component.name == XSD_ANY_TYPE and \
                component.target_namespace != XSD_NAMESPACE
        elif isinstance(component, XsdGroup) and component.parent is not None:
            return component.parent.name == XSD_ANY_TYPE and \
                component.target_namespace != XSD_NAMESPACE
        return False


class ObservedBuilders(XsdBuilders):

    def __set_name__(self, cls, name):
        super().__set_name__(cls, name)

        for attr in chain(self.__dict__, self.__slots__):
            value = getattr(self, attr)
            if isinstance(value, dict):
                for k, v in value.items():
                    value[k] = SchemaObserver.observed_builder(v)
            elif isinstance(value, type):
                object.__setattr__(self, attr, SchemaObserver.observed_builder(value))


class ObservedXMLSchema10(XMLSchema10):
    builders = ObservedBuilders()


class ObservedXMLSchema11(XMLSchema11):
    xsd_builders = ObservedBuilders()
