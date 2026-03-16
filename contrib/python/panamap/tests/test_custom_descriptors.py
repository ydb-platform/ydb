from typing import Type, Any, Set, Callable, Dict
from unittest import TestCase

from panamap import Mapper, MappingDescriptor
from panamap.panamap import T


class CustomDescriptorInitialized(Exception):
    pass


class CustomDescriptorSpecialClass:
    pass


class CustomDescriptor(MappingDescriptor):
    def __init__(self, t: Type[T]):
        super(CustomDescriptor, self).__init__(t)
        raise CustomDescriptorInitialized()

    @classmethod
    def supports_type(cls, t: Type[Any]) -> bool:
        return t is CustomDescriptorSpecialClass

    def get_getter(self, field_name: str) -> Callable[[T], Any]:
        pass

    def get_setter(self, field_name: str) -> Callable[[Dict, Any], None]:
        pass

    def get_constructor_args(self) -> Set[str]:
        pass

    def get_required_constructor_args(self) -> Set[str]:
        pass

    def get_declared_fields(self) -> Set[str]:
        pass

    def is_field_supported(self, field_name: str) -> bool:
        pass

    def get_preferred_field_type(self, field_name: str) -> Type[Any]:
        pass

    def is_container_type(self) -> bool:
        pass


class TestCustomDescriptors(TestCase):
    def test_custom_descriptor_loads(self):
        mapper = Mapper(custom_descriptors=[CustomDescriptor])
        with self.assertRaises(CustomDescriptorInitialized):
            mapper.mapping(CustomDescriptorSpecialClass, dict).map_matching().register()
