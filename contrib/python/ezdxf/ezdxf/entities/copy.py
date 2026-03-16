# Copyright (c) 2023, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, TypeVar, NamedTuple, Optional
from copy import deepcopy
import logging
from ezdxf.lldxf.const import DXFError

if TYPE_CHECKING:
    from ezdxf.entities import DXFEntity


__all__ = ["CopyStrategy", "CopySettings", "CopyNotSupported", "default_copy"]


T = TypeVar("T", bound="DXFEntity")


class CopyNotSupported(DXFError):
    pass


class CopySettings(NamedTuple):
    reset_handles: bool = True
    copy_extension_dict: bool = True
    copy_xdata: bool = True
    copy_appdata: bool = True
    copy_reactors: bool = False
    copy_proxy_graphic: bool = True
    set_source_of_copy: bool = True

    # The processing of copy errors of linked entities has to be done in the
    # copy_data() method by the entity itself!
    ignore_copy_errors_in_linked_entities: bool = True


class LogMessage(NamedTuple):
    message: str
    level: int = logging.WARNING
    entity: Optional[DXFEntity] = None


class CopyStrategy:
    log: list[LogMessage] = []

    def __init__(self, settings: CopySettings) -> None:
        self.settings = settings

    def copy(self, entity: T) -> T:
        """Entity copy for usage in the same document or as virtual entity.

        This copy is NOT stored in the entity database and does NOT reside in any
        layout, block, table or objects section!
        """
        settings = self.settings
        clone = entity.__class__()
        doc = entity.doc
        clone.doc = doc
        clone.dxf = entity.dxf.copy(clone)
        if settings.reset_handles:
            clone.dxf.reset_handles()

        if settings.copy_extension_dict:
            xdict = entity.extension_dict
            if xdict is not None and doc is not None and xdict.is_alive:
                # Error handling of unsupported entities in the extension dictionary is
                # done by the underlying DICTIONARY entity.
                clone.extension_dict = xdict.copy(self)

        if settings.copy_reactors and entity.reactors is not None:
            clone.reactors = entity.reactors.copy()

        if settings.copy_proxy_graphic:
            clone.proxy_graphic = entity.proxy_graphic  # immutable bytes

        # if appdata contains handles, they are treated as shared resources
        if settings.copy_appdata:
            clone.appdata = deepcopy(entity.appdata)

        # if xdata contains handles, they are treated as shared resources
        if settings.copy_xdata:
            clone.xdata = deepcopy(entity.xdata)

        if settings.set_source_of_copy:
            clone.set_source_of_copy(entity)

        entity.copy_data(clone, copy_strategy=self)
        return clone

    @classmethod
    def add_log_message(
        cls, msg: str, level: int = logging.WARNING, entity: Optional[DXFEntity] = None
    ) -> None:
        cls.log.append(LogMessage(msg, level, entity))

    @classmethod
    def clear_log_message(cls) -> None:
        cls.log.clear()


# same strategy as DXFEntity.copy() of v1.1.3
default_copy = CopyStrategy(CopySettings())
