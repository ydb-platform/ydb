#  Copyright (c) 2021-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Iterable, Optional, Iterator
from collections import Counter

from ezdxf.lldxf.types import POINTER_CODES
from ezdxf.protocols import referenced_blocks

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.lldxf.tags import Tags
    from ezdxf.entities import DXFEntity, BlockRecord

__all__ = ["BlockDefinitionIndex", "BlockReferenceCounter", "find_unreferenced_blocks"]

""" 
Where are block references located:

- HEADER SECTION: $DIMBLK, $DIMBLK1, $DIMBLK2, $DIMLDRBLK 
- DIMENSION: arrows referenced in the associated anonymous BLOCK, covered by 
  the INSERT entities in that BLOCK
- ACAD_TABLE: has an anonymous BLOCK representation, covered by the 
  INSERT entities in that BLOCK
- LEADER: DIMSTYLE override "dimldrblk" is stored as handle in XDATA

Entity specific block references, returned by the "ReferencedBlocks" protocol:
- INSERT: "name"
- DIMSTYLE: "dimblk", "dimblk1", "dimblk2", "dimldrblk"
- MLEADER: arrows, blocks - has no anonymous BLOCK representation
- MLEADERSTYLE: arrows
- DIMENSION: "geometry", the associated anonymous BLOCK
- ACAD_TABLE: currently managed as generic DXFTagStorage, that should return 
  the group code 343 "block_record" 

Possible unknown or undocumented block references:
- DXFTagStorage - all handle group codes
- XDATA - only group code 1005
- APPDATA - all handle group codes
- XRECORD - all handle group codes

Contains no block references as far as known:
- DICTIONARY: can only references DXF objects like XRECORD or DICTIONARYVAR
- Extension Dictionary is a DICTIONARY object
- REACTORS - used for object messaging, a reactor does not establish 
  a block reference

Block references are stored as handles to the BLOCK_RECORD entity!

Testing DXF documents with missing BLOCK definitions:

- INSERT without an existing BLOCK definition does NOT crash AutoCAD/BricsCAD
- HEADER variables $DIMBLK, $DIMBLK2, $DIMBLK2 and $DIMLDRBLK can reference 
  non existing blocks without crashing AutoCAD/BricsCAD  

"""


class BlockDefinitionIndex:
    """Index of all :class:`~ezdxf.entities.BlockRecord` entities representing
    real BLOCK definitions, excluding all :class:`~ezdxf.entities.BlockRecord`
    entities defining model space or paper space layouts. External references
    (XREF) and XREF overlays are included.

    """
    def __init__(self, doc: Drawing):
        self._doc = doc
        # mapping: handle -> BlockRecord entity
        self._handle_index: dict[str, BlockRecord] = dict()
        # mapping: block name -> BlockRecord entity
        self._name_index: dict[str, BlockRecord] = dict()
        self.rebuild()

    @property
    def block_records(self) -> Iterator[BlockRecord]:
        """Returns an iterator of all :class:`~ezdxf.entities.BlockRecord`
        entities representing BLOCK definitions.
        """
        return iter(self._doc.tables.block_records)

    def rebuild(self):
        """Rebuild index from scratch."""
        handle_index = self._handle_index
        name_index = self._name_index
        handle_index.clear()
        name_index.clear()
        for block_record in self.block_records:
            if block_record.is_block_layout:
                handle_index[block_record.dxf.handle] = block_record
                name_index[block_record.dxf.name] = block_record

    def has_handle(self, handle: str) -> bool:
        """Returns ``True`` if a :class:`~ezdxf.entities.BlockRecord` for the
        given block record handle exist.
        """
        return handle in self._handle_index

    def has_name(self, name: str) -> bool:
        """Returns ``True`` if a :class:`~ezdxf.entities.BlockRecord` for the
        given block name exist.
        """
        return name in self._name_index

    def by_handle(self, handle: str) -> Optional[BlockRecord]:
        """Returns the :class:`~ezdxf.entities.BlockRecord` for the given block
        record handle or ``None``.
        """
        return self._handle_index.get(handle)

    def by_name(self, name: str) -> Optional[BlockRecord]:
        """Returns :class:`~ezdxf.entities.BlockRecord` for the given block name
        or ``None``.
        """
        return self._name_index.get(name)


class BlockReferenceCounter:
    """
    Counts all block references in a DXF document.

    Check if a block is referenced by any entity or any resource (DIMSYTLE,
    MLEADERSTYLE) in a DXF document::

        import ezdxf
        from ezdxf.blkrefs import BlockReferenceCounter

        doc = ezdxf.readfile("your.dxf")
        counter = BlockReferenceCounter(doc)
        count = counter.by_name("XYZ")
        print(f"Block 'XYZ' if referenced {count} times.")

    """

    def __init__(self, doc: Drawing, index: Optional[BlockDefinitionIndex] = None):
        # mapping: handle -> BlockRecord entity
        self._block_record_index = (
            index if index is not None else BlockDefinitionIndex(doc)
        )

        # mapping: handle -> reference count
        self._counter = count_references(
            doc.entitydb.values(), self._block_record_index
        )
        self._counter.update(header_section_handles(doc))

    def by_handle(self, handle: str) -> int:
        """Returns the block reference count for a given
        :class:`~ezdxf.entities.BlockRecord` handle.
        """
        return self._counter[handle]

    def by_name(self, block_name: str) -> int:
        """Returns the block reference count for a given block name."""
        handle = ""
        block_record = self._block_record_index.by_name(block_name)
        if block_record is not None:
            handle = block_record.dxf.handle
        return self._counter[handle]


def count_references(
    entities: Iterable[DXFEntity], index: BlockDefinitionIndex
) -> Counter:
    from ezdxf.entities import XRecord, DXFTagStorage

    def update(handles: Iterable[str]):
        # only count references to existing blocks:
        counter.update(h for h in handles if index.has_handle(h))

    counter: Counter = Counter()
    for entity in entities:
        # add handles stored in XDATA and APP data
        update(generic_handles(entity))
        # add entity specific block references
        update(referenced_blocks(entity))
        # special entity types storing arbitrary raw DXF tags:
        if isinstance(entity, XRecord):
            update(all_pointer_handles(entity.tags))
        elif isinstance(entity, DXFTagStorage):
            # XDATA and APP data is already done!
            for tags in entity.xtags.subclasses[1:]:
                update(all_pointer_handles(tags))
            # ignore embedded objects: special objects for MTEXT and ATTRIB
    return counter


def generic_handles(entity: DXFEntity) -> Iterable[str]:
    handles: list[str] = []
    if entity.xdata is not None:
        for tags in entity.xdata.data.values():
            handles.extend(value for code, value in tags if code == 1005)
    if entity.appdata is not None:
        for tags in entity.appdata.data.values():
            handles.extend(all_pointer_handles(tags))
    return handles


def all_pointer_handles(tags: Tags) -> Iterable[str]:
    return (value for code, value in tags if code in POINTER_CODES)


def header_section_handles(doc: "Drawing") -> Iterable[str]:
    header = doc.header
    for var_name in ("$DIMBLK", "$DIMBLK1", "$DIMBLK2", "$DIMLDRBLK"):
        blk_name = header.get(var_name, None)
        if blk_name is not None:
            block = doc.blocks.get(blk_name, None)
            if block is not None:
                yield block.block_record.dxf.handle


def find_unreferenced_blocks(doc: Drawing) -> set[str]:
    """Returns the names of all block definitions without references.

    .. warning::

        The DXF reference does not document all uses of blocks. The INSERT entity is
        just one explicit use case, but there are also many indirect block references
        and the customizability of DXF allows you to store block names and handles in
        many places.

        There are some rules for storing names and handles and this module checks all of
        these known rules, but there is no guarantee that everyone follows these rules.

        Therefore, it is still possible to destroy a DXF document by deleting an
        absolutely necessary block definition.

    .. versionadded:: 1.3.5

    """
    ref_counter = BlockReferenceCounter(doc)
    unreferenced_blocks: set[str] = set()

    for block in doc.blocks:
        if not block.is_alive or block.is_any_layout:
            continue
        count = ref_counter.by_name(block.name)
        if count == 0:
            unreferenced_blocks.add(block.name)
    return unreferenced_blocks
