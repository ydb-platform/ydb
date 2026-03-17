# Copyright (c) 2011-2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import (
    Optional,
    TYPE_CHECKING,
    Iterable,
    Iterator,
    Callable,
    Any,
    Union,
    NewType,
    cast,
    NamedTuple,
    Mapping,
)
from enum import Enum
from .const import DXFAttributeError, DXF12
import copy

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity


class DefSubclass(NamedTuple):
    name: Optional[str]
    attribs: dict[str, DXFAttr]


VIRTUAL_TAG = -666


class XType(Enum):
    """Extended Attribute Types"""

    point2d = 1  # 2D points only
    point3d = 2  # 3D points only
    any_point = 3  # 2D or 3D points
    callback = 4  # callback attribute


def group_code_mapping(
    subclass: DefSubclass, *, ignore: Optional[Iterable[int]] = None
) -> dict[int, Union[str, list[str]]]:
    # Unique group codes are stored as group_code <int>: name <str>
    # Duplicate group codes are stored as group_code <int>: [name1, name2, ...] <list>
    # The order of appearance is important, therefore also callback attributes
    # have to be included, but they should not be loaded into the DXF namespace.
    mapping: dict[int, Union[str, list[str]]] = dict()
    for name, dxfattrib in subclass.attribs.items():
        if dxfattrib.xtype == XType.callback:
            # Mark callback attributes for special treatment as invalid
            # Python name:
            name = "*" + name
        code = dxfattrib.code
        existing_data: Union[None, str, list[str]] = mapping.get(code)
        if existing_data is None:
            mapping[code] = name
        else:
            if isinstance(existing_data, str):
                existing_data = [existing_data]
                mapping[code] = existing_data
            assert isinstance(existing_data, list)
            existing_data.append(name)

    if ignore:
        # Mark these tags as "*IGNORE" to be ignored,
        # but they are not real callbacks! See POLYLINE for example!
        for code in ignore:
            mapping[code] = "*IGNORE"
    return mapping


def merge_group_code_mappings(*mappings: Mapping) -> dict[int, str]:
    merge_group_code_mapping: dict[int, str] = {}
    for index, mapping in enumerate(mappings):
        msg = f"{index}. mapping contains none unique group codes"
        if not all(isinstance(e, str) for e in mapping.values()):
            raise TypeError(msg)
        if any(k in merge_group_code_mapping for k in mapping.keys()):
            raise TypeError(msg)
        merge_group_code_mapping.update(mapping)
    return merge_group_code_mapping


# Unique object as marker
ReturnDefault = NewType("ReturnDefault", object)
RETURN_DEFAULT = cast(ReturnDefault, object())


class DXFAttr:
    """Represents a DXF attribute for an DXF entity, accessible by the
    DXF namespace :attr:`DXFEntity.dxf` like ``entity.dxf.color = 7``.
    This definitions are immutable by design not by implementation.

    Extended Attribute Types
    ------------------------

    - XType.point2d:  2D points only
    - XType.point3d:  3D point only
    - XType.any_point:  mixed 2D/3D point
    - XType.callback: Calls get_value(entity) to get the value of DXF
      attribute 'name', and calls set_value(entity, value) to set value
      of DXF attribute 'name'.

    See example definition: ezdxf.entities.dxfgfx.acdb_entity.

    """

    def __init__(
        self,
        code: int,
        xtype: Optional[XType] = None,
        default=None,
        optional: bool = False,
        dxfversion: str = DXF12,
        getter: str = "",
        setter: str = "",
        alias: str = "",
        validator: Optional[Callable[[Any], bool]] = None,
        fixer: Optional[Union[Callable[[Any], Any], None, ReturnDefault]] = None,
    ):

        # Attribute name set by DXFAttributes.__init__()
        self.name: str = ""

        # DXF group code
        self.code: int = code

        # Extended attribute type:
        self.xtype: Optional[XType] = xtype

        # DXF default value
        self.default: Any = default

        # If optional is True, this attribute will be exported to DXF files
        # only if the given value differs from default value.
        self.optional: bool = optional

        # This attribute is valid for all DXF versions starting from the
        # specified DXF version, default is DXF12 = 'AC1009'
        self.dxfversion: str = dxfversion

        # DXF entity getter method name for callback attributes
        self.getter: str = getter

        # DXF entity setter method name for callback attributes
        self.setter: str = setter

        # Alternative name for this attribute
        self.alias: str = alias

        # Returns True if given value is valid - the validator should be as
        # fast as possible!
        self.validator: Optional[Callable[[Any], bool]] = validator

        # Returns a fixed value for invalid attributes, the fixer is called
        # only if the validator returns False.
        if fixer is RETURN_DEFAULT:
            fixer = self._return_default
        # excluding ReturnDefault type
        self.fixer = cast(Optional[Callable[[Any], Any]], fixer)

    def _return_default(self, x: Any) -> Any:
        return self.default

    def __str__(self) -> str:
        return f"({self.name}, {self.code})"

    def __repr__(self) -> str:
        return "DXFAttr" + self.__str__()

    def get_callback_value(self, entity: DXFEntity) -> Any:
        """
        Executes a callback function in 'entity' to get a DXF value.

        Callback function is defined by self.getter as string.

        Args:
            entity: DXF entity

        Raises:
            AttributeError: getter method does not exist
            TypeError: getter is None

        Returns:
            DXF attribute value

        """
        try:
            return getattr(entity, self.getter)()
        except AttributeError:
            raise DXFAttributeError(
                f"DXF attribute {self.name}: invalid getter {self.getter}."
            )
        except TypeError:
            raise DXFAttributeError(f"DXF attribute {self.name} has no getter.")

    def set_callback_value(self, entity: DXFEntity, value: Any) -> None:
        """Executes a callback function in 'entity' to set a DXF value.

        Callback function is defined by self.setter as string.

        Args:
            entity: DXF entity
            value: DXF attribute value

        Raises:
            AttributeError: setter method does not exist
            TypeError: setter is None

        """
        try:
            getattr(entity, self.setter)(value)
        except AttributeError:
            raise DXFAttributeError(
                f"DXF attribute {self.name}: invalid setter {self.setter}."
            )
        except TypeError:
            raise DXFAttributeError(f"DXF attribute {self.name} has no setter.")

    def is_valid_value(self, value: Any) -> bool:
        if self.validator:
            return self.validator(value)
        else:
            return True


class DXFAttributes:
    __slots__ = ("_attribs",)

    def __init__(self, *subclassdefs: DefSubclass):
        self._attribs: dict[str, DXFAttr] = dict()
        for subclass in subclassdefs:
            for name, dxfattrib in subclass.attribs.items():
                dxfattrib.name = name
                self._attribs[name] = dxfattrib
                if dxfattrib.alias:
                    alias = copy.copy(dxfattrib)
                    alias.name = dxfattrib.alias
                    alias.alias = dxfattrib.name
                    self._attribs[dxfattrib.alias] = alias

    def __contains__(self, name: str) -> bool:
        return name in self._attribs

    def get(self, key: str) -> Optional[DXFAttr]:
        return self._attribs.get(key)

    def build_group_code_items(self, func=lambda x: True) -> Iterator[tuple[int, str]]:
        return (
            (attrib.code, name)
            for name, attrib in self._attribs.items()
            if attrib.code > 0 and func(name)
        )
