#  Copyright (c) 2022-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import cast, Sequence
from ezdxf.entities import DXFEntity, Body as DXFBody
from ezdxf.lldxf import const
from .entities import Body, export_sat, export_sab, load


def export_dxf(entity: DXFEntity, bodies: Sequence[Body]):
    """Store the :term:`ACIS` bodies in the given DXF entity. This is the
    recommended way to set ACIS data of DXF entities.

    The DXF entity has to be an ACIS based entity and inherit from
    :class:`ezdxf.entities.Body`. The entity has to be bound to a valid DXF
    document and the DXF version of the document has to be DXF R2000 or newer.

    Raises:
        DXFTypeError: invalid DXF entity type
        DXFValueError: invalid DXF document
        DXFVersionError: invalid DXF version

    """
    if not isinstance(entity, DXFBody):
        raise const.DXFTypeError(f"invalid DXF entity {entity.dxftype()}")
    body = cast(DXFBody, entity)
    doc = entity.doc
    if doc is None:
        raise const.DXFValueError("a valid DXF document is required")
    dxfversion = doc.dxfversion
    if dxfversion < const.DXF2000:
        raise const.DXFVersionError(f"invalid DXF version {dxfversion}")

    if dxfversion < const.DXF2013:
        body.sat = export_sat(bodies)
    else:
        body.sab = export_sab(bodies)


def load_dxf(entity: DXFEntity) -> list[Body]:
    """Load the :term:`ACIS` bodies from the given DXF entity. This is the
    recommended way to load ACIS data.

    The DXF entity has to be an ACIS based entity and inherit from
    :class:`ezdxf.entities.Body`. The entity has to be bound to a valid DXF
    document and the DXF version of the document has to be DXF R2000 or newer.

    Raises:
        DXFTypeError: invalid DXF entity type
        DXFValueError: invalid DXF document
        DXFVersionError: invalid DXF version

    .. warning::

        Only a limited count of :term:`ACIS` entities is supported, all
        unsupported entities are loaded as ``NONE_ENTITY`` and their data is
        lost. Exporting such ``NONE_ENTITIES`` will raise an :class:`ExportError`
        exception.

        To emphasize that again: **It is not possible to load and re-export
        arbitrary ACIS data!**

    """

    if not isinstance(entity, DXFBody):
        raise const.DXFTypeError(f"invalid DXF entity {entity.dxftype()}")
    body = cast(DXFBody, entity)
    doc = entity.doc
    if doc is None:
        raise const.DXFValueError("a valid DXF document is required")
    dxfversion = doc.dxfversion
    if dxfversion < const.DXF2000:
        raise const.DXFVersionError(f"invalid DXF version {dxfversion}")
    if body.has_binary_data:
        binary_data = body.sab
        if binary_data:
            return load(binary_data)
    else:
        text = body.sat
        if text:
            return load(text)
    return []
