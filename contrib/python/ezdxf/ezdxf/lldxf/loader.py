# Copyright (c) 2018-2022, Manfred Moitzi
# License: MIT License
from __future__ import annotations
import logging
from typing import Iterable, TYPE_CHECKING, Optional
from collections import OrderedDict

from .const import DXFStructureError
from .tags import group_tags, DXFTag, Tags
from .extendedtags import ExtendedTags
from ezdxf.entities import factory

if TYPE_CHECKING:
    from ezdxf.document import Drawing
    from ezdxf.entities import DXFEntity
    from ezdxf.eztypes import SectionDict

logger = logging.getLogger("ezdxf")


def load_dxf_structure(
    tagger: Iterable[DXFTag], ignore_missing_eof: bool = False
) -> SectionDict:
    """Divide input tag stream from tagger into DXF structure entities.
    Each DXF structure entity starts with a DXF structure (0, ...) tag,
    and ends before the next DXF structure tag.

    Generated structure:

    each entity is a Tags() object

    {
        # 1. section, HEADER section consist only of one entity
        'HEADER': [entity],
        'CLASSES': [entity, entity, ...],  # 2. section
        'TABLES': [entity, entity, ...],   # 3. section
        ...
        'OBJECTS': [entity, entity, ...],
    }

    {
        # HEADER section consist only of one entity
        'HEADER': [(0, 'SECTION'), (2, 'HEADER'), .... ],
        'CLASSES': [
            [(0, 'SECTION'), (2, 'CLASSES')],
            [(0, 'CLASS'), ...],
            [(0, 'CLASS'), ...]
        ],
        'TABLES': [
            [(0, 'SECTION'), (2, 'TABLES')],
            [(0, 'TABLE'), (2, 'VPORT')],
            [(0, 'VPORT'), ...],
            ... ,
            [(0, 'ENDTAB')]
        ],
        ...
        'OBJECTS': [
            [(0, 'SECTION'), (2, 'OBJECTS')],
            ... ,
        ]
    }

    Args:
        tagger: generates DXFTag() entities from input data
        ignore_missing_eof: raises DXFStructureError() if False and EOF tag is
            not present, set to True only in tests

    Returns:
        dict of sections, each section is a list of DXF structure entities
        as Tags() objects

    """

    def inside_section() -> bool:
        if len(section):
            return section[0][0] == (0, "SECTION")  # first entity, first tag
        return False

    def outside_section() -> bool:
        if len(section):
            return section[0][0] != (0, "SECTION")  # first entity, first tag
        return True

    sections: SectionDict = OrderedDict()
    section: list[Tags] = []
    eof = False
    # The structure checking here should not be changed, ezdxf expect a valid
    # DXF file, to load messy DXF files exist an (future) add-on
    # called 'recover'.

    for entity in group_tags(tagger):
        tag = entity[0]
        if tag == (0, "SECTION"):
            if inside_section():
                raise DXFStructureError("DXFStructureError: missing ENDSEC tag.")
            if len(section):
                logger.warning(
                    "DXF Structure Warning: found tags outside a SECTION, "
                    "ignored by ezdxf."
                )
            section = [entity]
        elif tag == (0, "ENDSEC"):
            # ENDSEC tag is not collected.
            if outside_section():
                raise DXFStructureError(
                    "DXFStructureError: found ENDSEC tag without previous "
                    "SECTION tag."
                )
            section_header = section[0]

            if len(section_header) < 2 or section_header[1].code != 2:
                raise DXFStructureError(
                    "DXFStructureError: missing required section NAME tag "
                    "(2, name) at start of section."
                )
            name_tag = section_header[1]
            sections[name_tag.value] = section  # type: ignore
            # Collect tags outside of sections, but ignore it.
            section = []
        elif tag == (0, "EOF"):
            # EOF tag is not collected.
            if eof:
                logger.warning("DXF Structure Warning: found more than one EOF tags.")
            eof = True
        else:
            section.append(entity)
    if inside_section():
        raise DXFStructureError("DXFStructureError: missing ENDSEC tag.")
    if not eof and not ignore_missing_eof:
        raise DXFStructureError("DXFStructureError: missing EOF tag.")
    return sections


def load_dxf_entities(
    entities: Iterable[Tags], doc: Optional[Drawing] = None
) -> Iterable[DXFEntity]:
    for entity in entities:
        yield factory.load(ExtendedTags(entity), doc)


def load_and_bind_dxf_content(sections: dict, doc: Drawing) -> None:
    # HEADER has no database entries.
    db = doc.entitydb
    for name in ["TABLES", "CLASSES", "ENTITIES", "BLOCKS", "OBJECTS"]:
        if name in sections:
            section = sections[name]
            for index, entity in enumerate(load_dxf_entities(section, doc)):
                handle = entity.dxf.get("handle")
                if handle and handle in db:
                    logger.warning(
                        f"Found non-unique entity handle #{handle}, data validation is required."
                    )
                # Replace Tags() by DXFEntity() objects
                section[index] = entity
                # Bind entities to the DXF document:
                factory.bind(entity, doc)
