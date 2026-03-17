# Copyright (c) 2019-2023 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Union, Optional
from ezdxf.lldxf.tags import Tags
from ezdxf.lldxf.const import DXFStructureError
from ezdxf.lldxf.const import (
    ACAD_XDICTIONARY,
    XDICT_HANDLE_CODE,
    APP_DATA_MARKER,
)
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entities import (
        Dictionary,
        DXFEntity,
        DXFObject,
        Placeholder,
        DictionaryVar,
        XRecord,
    )

__all__ = ["ExtensionDict"]


# Example for table head and -entries with extension dicts:
# AutodeskSamples\lineweights.dxf


class ExtensionDict:
    """Stores extended data of entities in app data 'ACAD_XDICTIONARY', app
    data contains just one entry to a hard-owned DICTIONARY objects, which is
    not shared with other entities, each entity copy has its own extension
    dictionary and the extension dictionary is destroyed when the owner entity
    is deleted from database.

    """

    __slots__ = ("_xdict",)

    def __init__(self, xdict: Union[str, Dictionary]):
        # 1st loading stage: xdict as string -> handle to dict
        # 2nd loading stage: xdict as DXF Dictionary
        self._xdict = xdict

    @property
    def dictionary(self) -> Dictionary:
        """Returns the underlying :class:`~ezdxf.entities.Dictionary` object."""
        xdict = self._xdict
        assert xdict is not None, "destroyed extension dictionary"
        assert not isinstance(xdict, str), f"dictionary handle #{xdict} not resolved"
        return xdict

    @property
    def handle(self) -> str:
        """Returns the handle of the underlying :class:`~ezdxf.entities.Dictionary`
        object.
        """
        return self.dictionary.dxf.handle

    def __getitem__(self, key: str):
        """Get self[key]."""
        return self.dictionary[key]

    def __setitem__(self, key: str, value):
        """Set self[key] to value.

        Only DXF objects stored in the OBJECTS section are allowed as content
        of the extension dictionary. DXF entities stored in layouts are not
        allowed.

        Raises:
            DXFTypeError: invalid DXF type

        """
        self.dictionary[key] = value

    def __delitem__(self, key: str):
        """Delete self[key], destroys referenced entity."""
        del self.dictionary[key]

    def __contains__(self, key: str):
        """Return `key` in self."""
        return key in self.dictionary

    def __len__(self):
        """Returns count of extension dictionary entries."""
        return len(self.dictionary)

    def keys(self):
        """Returns a :class:`KeysView` of all extension dictionary keys."""
        return self.dictionary.keys()

    def items(self):
        """Returns an :class:`ItemsView` for all extension dictionary entries as
        (key, entity) pairs. An entity can be a handle string if the entity
        does not exist.
        """
        return self.dictionary.items()

    def get(self, key: str, default=None) -> Optional[DXFEntity]:
        """Return extension dictionary entry `key`."""
        return self.dictionary.get(key, default)

    def discard(self, key: str) -> None:
        """Discard extension dictionary entry `key`."""
        return self.dictionary.discard(key)

    @classmethod
    def new(cls, owner_handle: str, doc: Drawing):
        xdict = doc.objects.add_dictionary(
            owner=owner_handle,
            # All data in the extension dictionary belongs only to the owner
            hard_owned=True,
        )
        return cls(xdict)

    def copy(self, copy_strategy=default_copy) -> ExtensionDict:
        """Deep copy of the extension dictionary all entries are virtual
        entities.
        """
        new_xdict = copy_strategy.copy(self.dictionary)
        return ExtensionDict(new_xdict)

    @property
    def is_alive(self):
        """Returns ``True`` if the underlying :class:`~ezdxf.entities.Dictionary`
        object is not deleted.
        """
        # Can not check if _xdict (as handle or Dictionary) really exist:
        return self._xdict is not None

    @property
    def has_valid_dictionary(self):
        """Returns ``True`` if the underlying :class:`~ezdxf.entities.Dictionary`
        really exist and is valid.
        """
        xdict = self._xdict
        if xdict is None or isinstance(xdict, str):
            return False
        return xdict.is_alive

    def update_owner(self, handle: str) -> None:
        """Update owner tag of underlying :class:`~ezdxf.entities.Dictionary`
        object.

        Internal API.
        """
        assert self.is_alive, "destroyed extension dictionary"
        self.dictionary.dxf.owner = handle

    @classmethod
    def from_tags(cls, tags: Tags):
        assert tags is not None
        # Expected DXF structure:
        # [(102, '{ACAD_XDICTIONARY', (360, handle), (102, '}')]
        if len(tags) != 3 or tags[1].code != XDICT_HANDLE_CODE:
            raise DXFStructureError("ACAD_XDICTIONARY error.")
        return cls(tags[1].value)

    def load_resources(self, doc: Drawing) -> None:
        handle = self._xdict
        assert isinstance(handle, str)
        self._xdict = doc.entitydb.get(handle)  # type: ignore

    def export_dxf(self, tagwriter: AbstractTagWriter) -> None:
        assert self._xdict is not None
        xdict = self._xdict
        handle = xdict if isinstance(xdict, str) else xdict.dxf.handle
        tagwriter.write_tag2(APP_DATA_MARKER, ACAD_XDICTIONARY)
        tagwriter.write_tag2(XDICT_HANDLE_CODE, handle)
        tagwriter.write_tag2(APP_DATA_MARKER, "}")

    def destroy(self):
        """Destroy the underlying :class:`~ezdxf.entities.Dictionary` object."""
        if self.has_valid_dictionary:
            self._xdict.destroy()
        self._xdict = None

    def add_dictionary(self, name: str, hard_owned: bool = True) -> Dictionary:
        """Create a new :class:`~ezdxf.entities.Dictionary` object as
        extension dictionary entry `name`.
        """
        dictionary = self.dictionary
        doc = dictionary.doc
        assert doc is not None, "valid DXF document required"
        new_dict = doc.objects.add_dictionary(
            owner=dictionary.dxf.handle,
            hard_owned=hard_owned,
        )
        dictionary[name] = new_dict
        return new_dict

    def add_xrecord(self, name: str) -> XRecord:
        """Create a new :class:`~ezdxf.entities.XRecord` object as
        extension dictionary entry `name`.
        """
        dictionary = self.dictionary
        doc = dictionary.doc
        assert doc is not None, "valid DXF document required"
        xrecord = doc.objects.add_xrecord(dictionary.dxf.handle)
        dictionary[name] = xrecord
        return xrecord

    def add_dictionary_var(self, name: str, value: str) -> DictionaryVar:
        """Create a new :class:`~ezdxf.entities.DictionaryVar` object as
        extension dictionary entry `name`.
        """
        dictionary = self.dictionary
        doc = dictionary.doc
        assert doc is not None, "valid DXF document required"
        dict_var = doc.objects.add_dictionary_var(dictionary.dxf.handle, value)
        dictionary[name] = dict_var
        return dict_var

    def add_placeholder(self, name: str) -> Placeholder:
        """Create a new :class:`~ezdxf.entities.Placeholder` object as
        extension dictionary entry `name`.
        """
        dictionary = self.dictionary
        doc = dictionary.doc
        assert doc is not None, "valid DXF document required"
        placeholder = doc.objects.add_placeholder(dictionary.dxf.handle)
        dictionary[name] = placeholder
        return placeholder

    def link_dxf_object(self, name: str, obj: DXFObject) -> None:
        """Link `obj` to the extension dictionary as entry `name`.

        Linked objects are owned by the extensions dictionary and therefore
        cannot be a graphical entity, which have to be owned by a
        :class:`~ezdxf.layouts.BaseLayout`.

        Raises:
            DXFTypeError: `obj` has invalid DXF type

        """
        self.dictionary.link_dxf_object(name, obj)
