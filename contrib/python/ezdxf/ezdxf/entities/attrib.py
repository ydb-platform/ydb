# Copyright (c) 2019-2024 Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional
from typing_extensions import Self
import copy

from ezdxf.audit import Auditor, AuditError
from ezdxf.lldxf import validator
from ezdxf.math import NULLVEC, Vec3, Z_AXIS, OCS, Matrix44
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    XType,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf.lldxf import const
from ezdxf.lldxf.types import EMBEDDED_OBJ_MARKER, EMBEDDED_OBJ_STR
from ezdxf.enums import MAP_MTEXT_ALIGN_TO_FLAGS, TextHAlign, TextVAlign
from ezdxf.tools import set_flag_state
from ezdxf.tools.text import (
    load_mtext_content,
    fast_plain_mtext,
    plain_mtext,
)

from .dxfns import SubclassProcessor, DXFNamespace
from .dxfentity import base_class
from .dxfgfx import acdb_entity, elevation_to_z_axis
from .text import Text, acdb_text, acdb_text_group_codes
from .mtext import (
    acdb_mtext_group_codes,
    MText,
    export_mtext_content,
    acdb_mtext,
)
from .factory import register_entity
from .copy import default_copy

if TYPE_CHECKING:
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.lldxf.tags import Tags
    from ezdxf import xref


__all__ = ["AttDef", "Attrib", "copy_attrib_as_text", "BaseAttrib"]

# Where is it valid to place an ATTRIB entity:
# - YES: attached to an INSERT entity
# - NO: stand-alone entity in model space - ignored by BricsCAD and TrueView
# - NO: stand-alone entity in paper space - ignored by BricsCAD and TrueView
# - NO: stand-alone entity in block layout - ignored by BricsCAD and TrueView
#
# The RECOVER command of BricsCAD removes the stand-alone ATTRIB entities:
# "Invalid subentity type AcDbAttribute(<handle>)"
#
# IMPORTANT: placing ATTRIB at an invalid layout does NOT create an invalid DXF file!
#
# Where is it valid to place an ATTDEF entity:
# - NO: attached to an INSERT entity
# - YES: stand-alone entity in a BLOCK layout - BricsCAD and TrueView render the
#        TAG in the block editor and does not render the ATTDEF as block content
#        for the INSERT entity.
# - YES: stand-alone entity in model space - BricsCAD and TrueView render the
#        TAG not the default text - the model space is also a block content
#        (XREF, see also INSERT entity)
# - YES: stand-alone entity in paper space - same as model space, although a
#        paper space can not be used as XREF.

# DXF Reference for ATTRIB is a total mess and incorrect, the AcDbText subclass
# for the ATTRIB entity is the same as for the TEXT entity, but the valign field
# from the 2nd AcDbText subclass of the TEXT entity is stored in the
# AcDbAttribute subclass:
attrib_fields = {
    # "version": DXFAttr(280, default=0, dxfversion=const.DXF2010),
    # The "version" tag has the same group code as the lock_position tag!!!!!
    # Version number: 0 = 2010
    # This tag is not really used (at least by BricsCAD) but there exists DXF files
    # which do use this tag: "dxftest\attrib\attrib_with_mtext_R2018.dxf"
    # ezdxf stores the last group code 280 as "lock_position" attribute and does
    # not export a version tag for any DXF version.
    # Tag string (cannot contain spaces):
    # Mandatory by AutoCAD!
    "tag": DXFAttr(
        2,
        default="",
        validator=validator.is_valid_attrib_tag,
        fixer=validator.fix_attrib_tag,
    ),
    # 1 = Attribute is invisible (does not appear)
    # 2 = This is a constant attribute
    # 4 = Verification is required on input of this attribute
    # 8 = Attribute is preset (no prompt during insertion)
    "flags": DXFAttr(70, default=0),
    # Field length (optional) (not currently used)
    "field_length": DXFAttr(73, default=0, optional=True),
    # Vertical text justification type (optional); see group code 73 in TEXT
    "valign": DXFAttr(
        74,
        default=0,
        optional=True,
        validator=validator.is_in_integer_range(0, 4),
        fixer=RETURN_DEFAULT,
    ),
    # Lock position flag. Locks the position of the attribute within the block
    # reference, example of double use of group codes in one sub class
    "lock_position": DXFAttr(
        280,
        default=0,
        dxfversion=const.DXF2007,  # tested with BricsCAD 2023/TrueView 2023
        optional=True,
        validator=validator.is_integer_bool,
        fixer=RETURN_DEFAULT,
    ),
    # Attribute type:
    # 1 = single line
    # 2 = multiline ATTRIB
    # 4 = multiline ATTDEF
    "attribute_type": DXFAttr(
        71,
        default=const.ATTRIB_TYPE_SINGLE_LINE,
        dxfversion=const.DXF2018,
        optional=True,
        validator=validator.is_one_of({1, 2, 4}),
        fixer=RETURN_DEFAULT,
    ),
}

# ATTDEF has an additional field: 'prompt'
# DXF attribute definitions are immutable, a shallow copy is sufficient:
attdef_fields = dict(attrib_fields)
attdef_fields["prompt"] = DXFAttr(
    3,
    default="",
    validator=validator.is_valid_one_line_text,
    fixer=validator.fix_one_line_text,
)

acdb_attdef = DefSubclass("AcDbAttributeDefinition", attdef_fields)
acdb_attdef_group_codes = group_code_mapping(acdb_attdef)
acdb_attrib = DefSubclass("AcDbAttribute", attrib_fields)
acdb_attrib_group_codes = group_code_mapping(acdb_attrib)

# --------------------------------------------------------------------------------------
# Does subclass AcDbXrecord really exist? Only the documentation in the DXF reference
# exists, no real world examples seen so far - it wouldn't be the first error or misleading
# information in the DXF reference.
# --------------------------------------------------------------------------------------
# For XRECORD the tag order is important and group codes appear multiple times,
# therefore this attribute definition needs a special treatment!
acdb_attdef_xrecord = DefSubclass(
    "AcDbXrecord",
    [  # type: ignore
        # Duplicate record cloning flag (determines how to merge duplicate entries):
        # 1 = Keep existing
        ("cloning", DXFAttr(280, default=1)),
        # MText flag:
        # 2 = multiline attribute
        # 4 = constant multiline attribute definition
        ("mtext_flag", DXFAttr(70, default=0)),
        # isReallyLocked flag:
        #     0 = unlocked
        #     1 = locked
        (
            "really_locked",
            DXFAttr(
                70,
                default=0,
                validator=validator.is_integer_bool,
                fixer=RETURN_DEFAULT,
            ),
        ),
        # Number of secondary attributes or attribute definitions:
        ("secondary_attribs_count", DXFAttr(70, default=0)),
        # Hard-pointer id of secondary attribute(s) or attribute definition(s):
        ("secondary_attribs_handle", DXFAttr(340, default="0")),
        # Alignment point of attribute or attribute definition:
        ("align_point", DXFAttr(10, xtype=XType.point3d, default=NULLVEC)),
        ("current_annotation_scale", DXFAttr(40, default=0)),
        # attribute or attribute definition tag string
        (
            "tag",
            DXFAttr(
                2,
                default="",
                validator=validator.is_valid_attrib_tag,
                fixer=validator.fix_attrib_tag,
            ),
        ),
    ],
)


# Just for documentation:
# The "attached" MTEXT feature most likely does not exist!
#
#   A special MTEXT entity can follow the ATTDEF and ATTRIB entity, which starts
#   as a usual DXF entity with (0, 'MTEXT'), so processing can't be done here,
#   because for ezdxf is this a separated Entity.
#
#   The attached MTEXT entity: owner is None and handle is None
#   Linked as attribute `attached_mtext`.
#   I don't have seen this combination of entities in real world examples and is
#   ignored by ezdxf for now.
#
# No DXF files available which uses this feature - misleading DXF Reference!?

# Attrib and Attdef can have embedded MTEXT entities located in the
# <Embedded Object> subclass, see issue #258


class BaseAttrib(Text):
    XRECORD_DEF = acdb_attdef_xrecord

    def __init__(self) -> None:
        super().__init__()
        # Does subclass AcDbXrecord really exist?
        self._xrecord: Optional[Tags] = None
        self._embedded_mtext: Optional[EmbeddedMText] = None

    def copy_data(self, entity: Self, copy_strategy=default_copy) -> None:
        """Copy entity data, xrecord data and embedded MTEXT are not stored
        in the entity database.
        """
        assert isinstance(entity, BaseAttrib)
        entity._xrecord = copy.deepcopy(self._xrecord)
        entity._embedded_mtext = copy.deepcopy(self._embedded_mtext)

    def load_embedded_mtext(self, processor: SubclassProcessor) -> None:
        if not processor.embedded_objects:
            return
        embedded_object = processor.embedded_objects[0]
        if embedded_object:
            mtext = EmbeddedMText()
            mtext.load_dxf_tags(processor)
            self._embedded_mtext = mtext

    def export_dxf_r2018_features(self, tagwriter: AbstractTagWriter) -> None:
        tagwriter.write_tag2(71, self.dxf.attribute_type)
        tagwriter.write_tag2(72, 0)  # unknown tag
        if self.dxf.hasattr("align_point"):
            # duplicate align point - why?
            tagwriter.write_vertex(11, self.dxf.align_point)

        if self._xrecord:
            tagwriter.write_tags(self._xrecord)
        if self._embedded_mtext:
            self._embedded_mtext.export_dxf_tags(tagwriter)

    @property
    def is_const(self) -> bool:
        """This is a constant attribute if ``True``."""
        return bool(self.dxf.flags & const.ATTRIB_CONST)

    @is_const.setter
    def is_const(self, state: bool) -> None:
        self.dxf.flags = set_flag_state(self.dxf.flags, const.ATTRIB_CONST, state)

    @property
    def is_invisible(self) -> bool:
        """Attribute is invisible if ``True``."""
        return bool(self.dxf.flags & const.ATTRIB_INVISIBLE)

    @is_invisible.setter
    def is_invisible(self, state: bool) -> None:
        self.dxf.flags = set_flag_state(self.dxf.flags, const.ATTRIB_INVISIBLE, state)

    @property
    def is_verify(self) -> bool:
        """Verification is required on input of this attribute. (interactive CAD
        application feature)
        """
        return bool(self.dxf.flags & const.ATTRIB_VERIFY)

    @is_verify.setter
    def is_verify(self, state: bool) -> None:
        self.dxf.flags = set_flag_state(self.dxf.flags, const.ATTRIB_VERIFY, state)

    @property
    def is_preset(self) -> bool:
        """No prompt during insertion. (interactive CAD application feature)"""
        return bool(self.dxf.flags & const.ATTRIB_IS_PRESET)

    @is_preset.setter
    def is_preset(self, state: bool) -> None:
        self.dxf.flags = set_flag_state(self.dxf.flags, const.ATTRIB_IS_PRESET, state)

    @property
    def has_embedded_mtext_entity(self) -> bool:
        """Returns ``True`` if the entity has an embedded MTEXT entity for multi-line
        support.
        """
        return bool(self._embedded_mtext)

    def virtual_mtext_entity(self) -> MText:
        """Returns the embedded MTEXT entity as a regular but virtual
        :class:`MText` entity with the same graphical properties as the
        host entity.
        """
        if not self._embedded_mtext:
            raise TypeError("no embedded MTEXT entity exist")
        mtext = self._embedded_mtext.virtual_mtext_entity()
        mtext.update_dxf_attribs(self.graphic_properties())
        return mtext

    def plain_mtext(self, fast=True) -> str:
        """Returns the embedded MTEXT content without formatting codes.
        Returns an empty string if no embedded MTEXT entity exist.

        The `fast` mode is accurate if the DXF content was created by
        reliable (and newer) CAD applications like AutoCAD or BricsCAD.
        The `accurate` mode is for some rare cases where the content was
        created by older CAD applications or unreliable DXF libraries and CAD
        applications.

        The `accurate` mode is **much** slower than the `fast` mode.

        Args:
            fast: uses the `fast` mode to extract the plain MTEXT content if
                ``True`` or the `accurate` mode if set to ``False``

        """
        if self._embedded_mtext:
            text = self._embedded_mtext.text
            if fast:
                return fast_plain_mtext(text, split=False)  # type: ignore
            else:
                return plain_mtext(text, split=False)  # type: ignore
        return ""

    def set_mtext(self, mtext: MText, graphic_properties=True) -> None:
        """Set multi-line properties from a :class:`MText` entity.

        The multi-line ATTRIB/ATTDEF entity requires DXF R2018, otherwise an
        ordinary single line ATTRIB/ATTDEF entity will be exported.

        Args:
            mtext: source :class:`MText` entity
            graphic_properties: copy graphic properties (color, layer, ...) from
                source MTEXT if ``True``

        """
        if self._embedded_mtext is None:
            self._embedded_mtext = EmbeddedMText()
        self._embedded_mtext.set_mtext(mtext)
        _update_content_from_mtext(self, mtext)
        _update_location_from_mtext(self, mtext)

        # set attribute type:
        if isinstance(self, Attrib):
            attribute_type = const.ATTRIB_TYPE_MULTI_LINE
        else:
            attribute_type = const.ATTDEF_TYPE_MULTI_LINE
        self.dxf.attribute_type = attribute_type

        # misc properties
        self.dxf.style = mtext.dxf.style
        self.dxf.height = mtext.dxf.char_height
        self.dxf.discard("width")  # controlled in MTEXT by inline codes!
        self.dxf.discard("oblique")  # controlled in MTEXT by inline codes!
        self.dxf.discard("text_generation_flag")
        if graphic_properties:
            self.update_dxf_attribs(mtext.graphic_properties())

    def embed_mtext(self, mtext: MText, graphic_properties=True) -> None:
        """Set multi-line properties from a :class:`MText` entity and destroy the
        source entity afterward.

        The multi-line ATTRIB/ATTDEF entity requires DXF R2018, otherwise an
        ordinary single line ATTRIB/ATTDEF entity will be exported.

        Args:
            mtext: source :class:`MText` entity
            graphic_properties: copy graphic properties (color, layer, ...) from
                source MTEXT if ``True``

        """
        self.set_mtext(mtext, graphic_properties)
        mtext.destroy()

    def discard_mtext(self) -> None:
        """Discard multi-line feature.

        The embedded MTEXT will be removed and the ATTRIB/ATTDEF will be converted to a
        single-line attribute.
        """
        self._embedded_mtext = None
        self.dxf.attribute_type = const.ATTRIB_TYPE_SINGLE_LINE

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        super().register_resources(registry)
        if self._embedded_mtext:
            self._embedded_mtext.register_resources(registry)

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, BaseAttrib)
        super().map_resources(clone, mapping)
        if self._embedded_mtext and clone._embedded_mtext:
            self._embedded_mtext.map_resources(clone._embedded_mtext, mapping)
        # todo: map handles in embedded XRECORD if a real world example shows up

    def transform(self, m: Matrix44) -> Self:
        if self._embedded_mtext is None:
            super().transform(m)
        else:
            mtext = self._embedded_mtext.virtual_mtext_entity()
            mtext.transform(m)
            self.set_mtext(mtext, graphic_properties=False)
            self.post_transform(m)
        return self

    def audit(self, auditor: Auditor) -> None:
        """Validity check."""
        super().audit(auditor)
        if not self.dxf.hasattr("tag"):
            auditor.fixed_error(
                code=AuditError.TAG_ATTRIBUTE_MISSING,
                message=f'Missing mandatory "tag" attribute, entity {str(self)} deleted.',
            )
            auditor.trash(self)


def _update_content_from_mtext(text: Text, mtext: MText) -> None:
    content = mtext.plain_text(split=True, fast=True)
    if content:
        # In contrast to AutoCAD, just set the first line as single line
        # ATTRIB content. AutoCAD concatenates all lines into a single
        # "Line1\PLine2\P...", which (imho) is not very useful.
        text.dxf.text = content[0]


def _update_location_from_mtext(text: Text, mtext: MText) -> None:
    # TEXT is an OCS entity, MTEXT is a WCS entity
    dxf = text.dxf
    insert = Vec3(mtext.dxf.insert)
    extrusion = Vec3(mtext.dxf.extrusion)
    text_direction = mtext.get_text_direction()
    if extrusion.isclose(Z_AXIS):  # most common case
        dxf.rotation = text_direction.angle_deg
    else:
        ocs = OCS(extrusion)
        insert = ocs.from_wcs(insert)
        dxf.extrusion = extrusion.normalize()
        dxf.rotation = ocs.from_wcs(text_direction).angle_deg

    dxf.insert = insert
    dxf.align_point = insert  # the same point for all MTEXT alignments!
    dxf.halign, dxf.valign = MAP_MTEXT_ALIGN_TO_FLAGS.get(
        mtext.dxf.attachment_point, (TextHAlign.LEFT, TextVAlign.TOP)
    )


@register_entity
class AttDef(BaseAttrib):
    """DXF ATTDEF entity"""

    DXFTYPE = "ATTDEF"
    # Don't add acdb_attdef_xrecord here:
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_text, acdb_attdef)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super(Text, self).load_dxf_attribs(processor)
        # Do not call Text loader.
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_text_group_codes, 2, recover=True)
            processor.fast_load_dxfattribs(
                dxf, acdb_attdef_group_codes, 3, recover=True
            )
            self._xrecord = processor.find_subclass(self.XRECORD_DEF.name)  # type: ignore
            self.load_embedded_mtext(processor)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, ("insert", "align_point"))

        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        # Text() writes 2x AcDbText which is not suitable for AttDef()
        self.export_acdb_entity(tagwriter)
        self.export_acdb_text(tagwriter)
        self.export_acdb_attdef(tagwriter)
        if tagwriter.dxfversion >= const.DXF2018:
            self.export_dxf_r2018_features(tagwriter)

    def export_acdb_attdef(self, tagwriter: AbstractTagWriter) -> None:
        if tagwriter.dxfversion > const.DXF12:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_attdef.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                # write version tag (280, 0) here, if required in the future
                "prompt",
                "tag",
                "flags",
                "field_length",
                "valign",
                "lock_position",
            ],
        )


@register_entity
class Attrib(BaseAttrib):
    """DXF ATTRIB entity"""

    DXFTYPE = "ATTRIB"
    # Don't add acdb_attdef_xrecord here:
    DXFATTRIBS = DXFAttributes(base_class, acdb_entity, acdb_text, acdb_attrib)

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super(Text, self).load_dxf_attribs(processor)
        # Do not call Text loader.
        if processor:
            processor.fast_load_dxfattribs(dxf, acdb_text_group_codes, 2, recover=True)
            processor.fast_load_dxfattribs(
                dxf, acdb_attrib_group_codes, 3, recover=True
            )
            self._xrecord = processor.find_subclass(self.XRECORD_DEF.name)  # type: ignore
            self.load_embedded_mtext(processor)
            if processor.r12:
                # Transform elevation attribute from R11 to z-axis values:
                elevation_to_z_axis(dxf, ("insert", "align_point"))
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        # Text() writes 2x AcDbText which is not suitable for AttDef()
        self.export_acdb_entity(tagwriter)
        self.export_acdb_attrib_text(tagwriter)
        self.export_acdb_attrib(tagwriter)
        if tagwriter.dxfversion >= const.DXF2018:
            self.export_dxf_r2018_features(tagwriter)

    def export_acdb_attrib_text(self, tagwriter: AbstractTagWriter) -> None:
        # Despite the similarities to TEXT, it is different to
        # Text.export_acdb_text():
        if tagwriter.dxfversion > const.DXF12:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_text.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "insert",
                "height",
                "text",
                "thickness",
                "rotation",
                "oblique",
                "style",
                "width",
                "halign",
                "align_point",
                "text_generation_flag",
                "extrusion",
            ],
        )

    def export_acdb_attrib(self, tagwriter: AbstractTagWriter) -> None:
        if tagwriter.dxfversion > const.DXF12:
            tagwriter.write_tag2(const.SUBCLASS_MARKER, acdb_attrib.name)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                # write version tag (280, 0) here, if required in the future
                "tag",
                "flags",
                "field_length",
                "valign",
                "lock_position",
            ],
        )


IGNORE_FROM_ATTRIB = {
    "handle",
    "owner",
    "version",
    "prompt",
    "tag",
    "flags",
    "field_length",
    "lock_position",
}


def copy_attrib_as_text(attrib: BaseAttrib):
    """Returns the content of the ATTRIB/ATTDEF entity as a new virtual TEXT or
    MTEXT entity.

    """
    if attrib.has_embedded_mtext_entity:
        return attrib.virtual_mtext_entity()
    dxfattribs = attrib.dxfattribs(drop=IGNORE_FROM_ATTRIB)
    return Text.new(dxfattribs=dxfattribs, doc=attrib.doc)


class EmbeddedMTextNS(DXFNamespace):
    _DXFATTRIBS = DXFAttributes(acdb_mtext)

    @property
    def dxfattribs(self) -> DXFAttributes:
        return self._DXFATTRIBS

    @property
    def dxftype(self) -> str:
        return "Embedded MText"


class EmbeddedMText:
    """Representation of the embedded MTEXT object in ATTRIB and ATTDEF.

    Introduced in DXF R2018? The DXF reference of the `MTEXT`_ entity
    documents only the attached MTEXT entity. The ODA DWG specs includes all
    MTEXT attributes of MTEXT starting at group code 10

    Stores the required parameters to be shown as as MTEXT.
    The AcDbText subclass contains  the first line of the embedded MTEXT as
    plain text content as group code 1, but this tag seems not to be maintained
    if the ATTRIB entity is copied.

    Some DXF attributes are duplicated and maintained by the CAD application:

        - textstyle: same group code 7 (AcDbText, EmbeddedObject)
        - text (char) height: same group code 40 (AcDbText, EmbeddedObject)

    .. _MTEXT: https://help.autodesk.com/view/OARX/2018/ENU/?guid=GUID-7DD8B495-C3F8-48CD-A766-14F9D7D0DD9B

    """

    def __init__(self) -> None:
        # Attribute "dxf" contains the DXF attributes defined in subclass
        # "AcDbMText"
        self.dxf = EmbeddedMTextNS()
        self.text: str = ""

    def copy(self) -> EmbeddedMText:
        copy_ = EmbeddedMText()
        copy_.dxf = copy.deepcopy(self.dxf)
        return copy_

    __copy__ = copy

    def load_dxf_tags(self, processor: SubclassProcessor) -> None:
        tags = processor.fast_load_dxfattribs(
            self.dxf,
            group_code_mapping=acdb_mtext_group_codes,
            subclass=processor.embedded_objects[0],
            recover=False,
        )
        self.text = load_mtext_content(tags)

    def virtual_mtext_entity(self) -> MText:
        """Returns the embedded MTEXT entity as regular but virtual MTEXT
        entity. This entity does not have the graphical attributes of the host
        entity (ATTRIB/ATTDEF).

        """
        mtext = MText.new(dxfattribs=self.dxf.all_existing_dxf_attribs())
        mtext.text = self.text
        return mtext

    def set_mtext(self, mtext: MText) -> None:
        """Set embedded MTEXT attributes from given `mtext` entity."""
        self.text = mtext.text
        dxf = self.dxf
        for k, v in mtext.dxf.all_existing_dxf_attribs().items():
            if dxf.is_supported(k):
                dxf.set(k, v)

    def set_required_dxf_attributes(self):
        # These attributes are always present in DXF files created by Autocad:
        dxf = self.dxf
        for key, default in (
            ("insert", NULLVEC),
            ("char_height", 2.5),
            ("width", 0.0),
            ("defined_height", 0.0),
            ("attachment_point", 1),
            ("flow_direction", 5),
            ("style", "Standard"),
            ("line_spacing_style", 1),
            ("line_spacing_factor", 1.0),
        ):
            if not dxf.hasattr(key):
                dxf.set(key, default)

    def export_dxf_tags(self, tagwriter: AbstractTagWriter) -> None:
        """Export embedded MTEXT as "Embedded Object"."""
        tagwriter.write_tag2(EMBEDDED_OBJ_MARKER, EMBEDDED_OBJ_STR)
        self.set_required_dxf_attributes()
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "insert",
                "char_height",
                "width",
                "defined_height",
                "attachment_point",
                "flow_direction",
            ],
        )
        export_mtext_content(self.text, tagwriter)
        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "style",
                "extrusion",
                "text_direction",
                "rect_width",
                "rect_height",
                "rotation",
                "line_spacing_style",
                "line_spacing_factor",
                "box_fill_scale",
                "bg_fill",
                "bg_fill_color",
                "bg_fill_true_color",
                "bg_fill_color_name",
                "bg_fill_transparency",
            ],
        )

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        if self.dxf.hasattr("style"):
            registry.add_text_style(self.dxf.style)

    def map_resources(self, clone: EmbeddedMText, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        if clone.dxf.hasattr("style"):
            clone.dxf.style = mapping.get_text_style(clone.dxf.style)
