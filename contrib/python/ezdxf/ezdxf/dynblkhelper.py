#  Copyright (c) 2023, Manfred Moitzi
#  License: MIT License
"""
This module provides helper tools to work with dynamic blocks.

The current state supports only reading information from dynamic blocks, it does not
support the creation of new dynamic blocks nor the modification of them.

"""
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from ezdxf.entities import Insert
from ezdxf.lldxf import const

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.layouts import BlockLayout
    from ezdxf.entities import BlockRecord

AcDbDynamicBlockGUID = "AcDbDynamicBlockGUID"
AcDbBlockRepBTag = "AcDbBlockRepBTag"


def get_dynamic_block_definition(
    insert: Insert, doc: Optional[Drawing] = None
) -> Optional[BlockLayout]:
    """Returns the dynamic block definition if the given block reference is
    referencing a dynamic block direct or indirect via an anonymous block.
    Returns ``None`` otherwise.
    """
    if doc is None:
        doc = insert.doc
        if doc is None:
            return None

    block = doc.blocks.get(insert.dxf.name)
    if block is None:
        return None

    block_record = block.block_record
    if is_dynamic_block_definition(block_record):
        return block  # direct dynamic block reference

    # is indirect dynamic block reference?
    handle = get_dynamic_block_record_handle(block_record)
    if not handle:
        return None  # lost reference to dynamic block definition
    dyn_block_record = doc.entitydb.get(handle)
    if dyn_block_record:
        return doc.blocks.get(dyn_block_record.dxf.name)
    return None


def is_dynamic_block_definition(block_record: BlockRecord) -> bool:
    """Return ``True`` if the given block record is a dynamic block definition."""
    return block_record.has_xdata(AcDbDynamicBlockGUID)


def get_dynamic_block_record_handle(block_record: BlockRecord) -> str:
    """Returns handle of the dynamic block record for an indirect dynamic block
    reference. Returns an empty string if the block record do not reference a dynamic
    block or the handle was not found.

    """
    try:  # check for indirect dynamic block reference
        xdata = block_record.get_xdata(AcDbBlockRepBTag)
    except const.DXFValueError:
        return ""  # not a dynamic block reference
    # get handle of dynamic block definition
    return xdata.get_first_value(1005, "")
