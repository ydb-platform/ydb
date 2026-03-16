# Copyright (c) 2019-2024, Manfred Moitzi
# License: MIT License
from __future__ import annotations
from typing import TYPE_CHECKING, Optional, cast, Any
from typing_extensions import Self
import logging
from dataclasses import dataclass
from ezdxf.lldxf import validator
from ezdxf.lldxf.attributes import (
    DXFAttr,
    DXFAttributes,
    DefSubclass,
    RETURN_DEFAULT,
    group_code_mapping,
)
from ezdxf import colors as clr
from ezdxf.lldxf import const
from ezdxf.lldxf.const import (
    DXF12,
    SUBCLASS_MARKER,
    DXF2000,
    DXF2007,
    DXF2004,
    INVALID_NAME_CHARACTERS,
    DXFValueError,
    LINEWEIGHT_BYBLOCK,
    LINEWEIGHT_BYLAYER,
    LINEWEIGHT_DEFAULT,
)
from ezdxf.audit import AuditError
from ezdxf.entities.dxfentity import base_class, SubclassProcessor, DXFEntity
from .factory import register_entity

if TYPE_CHECKING:
    from ezdxf.entities import DXFNamespace, Viewport, XRecord
    from ezdxf.lldxf.tagwriter import AbstractTagWriter
    from ezdxf.entitydb import EntityDB
    from ezdxf import xref
    from ezdxf.audit import Auditor


__all__ = ["Layer", "acdb_symbol_table_record", "LayerOverrides"]
logger = logging.getLogger("ezdxf")


def is_valid_layer_color_index(aci: int) -> bool:
    # BYBLOCK or BYLAYER is not valid a layer color!
    return (-256 < aci < 256) and aci != 0


def fix_layer_color(aci: int) -> int:
    return aci if is_valid_layer_color_index(aci) else 7


def is_valid_layer_lineweight(lw: int) -> bool:
    if validator.is_valid_lineweight(lw):
        if lw not in (LINEWEIGHT_BYLAYER, LINEWEIGHT_BYBLOCK):
            return True
    return False


def fix_layer_lineweight(lw: int) -> int:
    if lw in (LINEWEIGHT_BYLAYER, LINEWEIGHT_BYBLOCK):
        return LINEWEIGHT_DEFAULT
    else:
        return validator.fix_lineweight(lw)


acdb_symbol_table_record: DefSubclass = DefSubclass("AcDbSymbolTableRecord", {})

acdb_layer_table_record = DefSubclass(
    "AcDbLayerTableRecord",
    {
        # Layer name as string
        "name": DXFAttr(2, validator=validator.is_valid_layer_name),
        "flags": DXFAttr(70, default=0),
        # ACI color index, color < 0 indicates layer status: off
        "color": DXFAttr(
            62,
            default=7,
            validator=is_valid_layer_color_index,
            fixer=fix_layer_color,
        ),
        # True color as 24 bit int value: 0x00RRGGBB
        "true_color": DXFAttr(420, dxfversion=DXF2004, optional=True),
        # Linetype name as string
        "linetype": DXFAttr(
            6, default="Continuous", validator=validator.is_valid_table_name
        ),
        # 0 = don't plot layer; 1 = plot layer
        "plot": DXFAttr(
            290,
            default=1,
            dxfversion=DXF2000,
            optional=True,
            validator=validator.is_integer_bool,
            fixer=RETURN_DEFAULT,
        ),
        # Default lineweight 1/100 mm, min 0 = 0.0mm, max 211 = 2.11mm
        "lineweight": DXFAttr(
            370,
            default=LINEWEIGHT_DEFAULT,
            dxfversion=DXF2000,
            validator=is_valid_layer_lineweight,
            fixer=fix_layer_lineweight,
        ),
        # Handle to PlotStyleName, group code 390 is required by AutoCAD
        "plotstyle_handle": DXFAttr(390, dxfversion=DXF2000),
        # Handle to Material object
        "material_handle": DXFAttr(347, dxfversion=DXF2007),
        # Handle to ???
        "unknown1": DXFAttr(348, dxfversion=DXF2007, optional=True),
    },
)
acdb_layer_table_record_group_codes = group_code_mapping(acdb_layer_table_record)
AcAecLayerStandard = "AcAecLayerStandard"
AcCmTransparency = "AcCmTransparency"


@register_entity
class Layer(DXFEntity):
    """DXF LAYER entity"""

    DXFTYPE = "LAYER"
    DXFATTRIBS = DXFAttributes(
        base_class, acdb_symbol_table_record, acdb_layer_table_record
    )
    DEFAULT_ATTRIBS = {"name": "0"}
    FROZEN = 0b00000001
    THAW = 0b11111110
    LOCK = 0b00000100
    UNLOCK = 0b11111011

    def load_dxf_attribs(
        self, processor: Optional[SubclassProcessor] = None
    ) -> DXFNamespace:
        dxf = super().load_dxf_attribs(processor)
        if processor:
            processor.simple_dxfattribs_loader(
                dxf, acdb_layer_table_record_group_codes  # type: ignore
            )
        return dxf

    def export_entity(self, tagwriter: AbstractTagWriter) -> None:
        super().export_entity(tagwriter)
        if tagwriter.dxfversion > DXF12:
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_symbol_table_record.name)
            tagwriter.write_tag2(SUBCLASS_MARKER, acdb_layer_table_record.name)

        self.dxf.export_dxf_attribs(
            tagwriter,
            [
                "name",
                "flags",
                "color",
                "true_color",
                "linetype",
                "plot",
                "lineweight",
                "plotstyle_handle",
                "material_handle",
                "unknown1",
            ],
        )

    def set_required_attributes(self):
        assert self.doc is not None, "valid DXF document required"
        if not self.dxf.hasattr("material_handle"):
            global_ = self.doc.materials["Global"]
            if isinstance(global_, DXFEntity):
                handle = global_.dxf.handle
            else:
                handle = global_
            self.dxf.material_handle = handle
        if not self.dxf.hasattr("plotstyle_handle"):
            normal = self.doc.plotstyles["Normal"]
            if isinstance(normal, DXFEntity):
                handle = normal.dxf.handle
            else:
                handle = normal
            self.dxf.plotstyle_handle = handle

    def is_frozen(self) -> bool:
        """Returns ``True`` if layer is frozen."""
        return self.dxf.flags & Layer.FROZEN > 0

    def freeze(self) -> None:
        """Freeze layer."""
        self.dxf.flags = self.dxf.flags | Layer.FROZEN

    def thaw(self) -> None:
        """Thaw layer."""
        self.dxf.flags = self.dxf.flags & Layer.THAW

    def is_locked(self) -> bool:
        """Returns ``True`` if layer is locked."""
        return self.dxf.flags & Layer.LOCK > 0

    def lock(self) -> None:
        """Lock layer, entities on this layer are not editable - just important
        in CAD applications.
        """
        self.dxf.flags = self.dxf.flags | Layer.LOCK

    def unlock(self) -> None:
        """Unlock layer, entities on this layer are editable - just important
        in CAD applications.
        """
        self.dxf.flags = self.dxf.flags & Layer.UNLOCK

    def is_off(self) -> bool:
        """Returns ``True`` if layer is off."""
        return self.dxf.color < 0

    def is_on(self) -> bool:
        """Returns ``True`` if layer is on."""
        return not self.is_off()

    def on(self) -> None:
        """Switch layer `on` (visible)."""
        self.dxf.color = abs(self.dxf.color)

    def off(self) -> None:
        """Switch layer `off` (invisible)."""
        self.dxf.color = -abs(self.dxf.color)

    def get_color(self) -> int:
        """Get layer color, safe method for getting the layer color, because
        :attr:`dxf.color` is negative for layer status `off`.
        """
        return abs(self.dxf.color)

    def set_color(self, color: int) -> None:
        """Set layer color, safe method for setting the layer color, because
        :attr:`dxf.color` is negative for layer status `off`.
        """
        color = abs(color) if self.is_on() else -abs(color)
        self.dxf.color = color

    @property
    def rgb(self) -> Optional[tuple[int, int, int]]:
        """Returns RGB true color as (r, g, b)-tuple or None if attribute
        dxf.true_color is not set.
        """
        if self.dxf.hasattr("true_color"):
            return clr.int2rgb(self.dxf.get("true_color"))
        else:
            return None

    @rgb.setter
    def rgb(self, rgb: tuple[int, int, int]) -> None:
        """Set RGB true color as (r, g, b)-tuple e.g. (12, 34, 56)."""
        self.dxf.set("true_color", clr.rgb2int(rgb))

    @property
    def color(self) -> int:
        """Get layer color, safe method for getting the layer color, because
        :attr:`dxf.color` is negative for layer status `off`.
        """
        return self.get_color()

    @color.setter
    def color(self, value: int) -> None:
        """Set layer color, safe method for setting the layer color, because
        :attr:`dxf.color` is negative for layer status `off`.
        """
        self.set_color(value)

    @property
    def description(self) -> str:
        try:
            xdata = self.get_xdata(AcAecLayerStandard)
        except DXFValueError:
            return ""
        else:
            if len(xdata) > 1:
                # this is the usual case in BricsCAD
                return xdata[1].value
            else:
                return ""

    @description.setter
    def description(self, value: str) -> None:
        # create AppID table entry if not present
        if self.doc and AcAecLayerStandard not in self.doc.appids:
            self.doc.appids.new(AcAecLayerStandard)
        self.discard_xdata(AcAecLayerStandard)
        self.set_xdata(AcAecLayerStandard, [(1000, ""), (1000, value)])

    @property
    def transparency(self) -> float:
        try:
            xdata = self.get_xdata(AcCmTransparency)
        except DXFValueError:
            return 0.0
        else:
            t = xdata[0].value
            if t & 0x2000000:  # is this a real transparency value?
                # Transparency BYBLOCK (0x01000000) make no sense for a layer!?
                return clr.transparency2float(t)
        return 0.0

    @transparency.setter
    def transparency(self, value: float) -> None:
        # create AppID table entry if not present
        if self.doc and AcCmTransparency not in self.doc.appids:
            self.doc.appids.new(AcCmTransparency)
        if 0 <= value <= 1:
            self.discard_xdata(AcCmTransparency)
            self.set_xdata(AcCmTransparency, [(1071, clr.float2transparency(value))])
        else:
            raise ValueError("Value out of range [0, 1].")

    def rename(self, name: str) -> None:
        """
        Rename layer and all known (documented) references to this layer.

        .. warning::

            The DXF format is not consistent in storing layer references, the
            layers are mostly referenced by their case-insensitive name,
            some later introduced entities do reference layers by handle, which
            is the safer way in the context of renaming layers.

            There is no complete overview of where layer references are
            stored, third-party entities are black-boxes with unknown content
            and layer names could be stored in the extended data section of any
            DXF entity or in XRECORD entities.
            Which means that in some rare cases references to the old layer name
            can persist, at least this does not invalidate the DXF document.

        Args:
             name: new layer name

        Raises:
            ValueError: `name` contains invalid characters: <>/\\":;?*|=`
            ValueError: layer `name` already exist
            ValueError: renaming of layers ``'0'`` and ``'DEFPOINTS'`` not
                possible

        """
        if not validator.is_valid_layer_name(name):
            raise ValueError(
                f"Name contains invalid characters: {INVALID_NAME_CHARACTERS}."
            )
        assert self.doc is not None, "valid DXF document is required"
        layers = self.doc.layers
        if self.dxf.name.lower() in ("0", "defpoints"):
            raise ValueError(f'Can not rename layer "{self.dxf.name}".')
        if layers.has_entry(name):
            raise ValueError(f'Layer "{name}" already exist.')
        old = self.dxf.name
        self.dxf.name = name
        layers.replace(old, self)
        self._rename_layer_references(old, name)

    def _rename_layer_references(self, old_name: str, new_name: str) -> None:
        assert self.doc is not None, "valid DXF document is required"
        key = self.doc.layers.key
        old_key = key(old_name)
        for e in self.doc.entitydb.values():
            if e.dxf.hasattr("layer") and key(e.dxf.layer) == old_key:
                e.dxf.layer = new_name
            entity_type = e.dxftype()
            if entity_type == "VIEWPORT":
                e.rename_frozen_layer(old_name, new_name)  # type: ignore
            elif entity_type == "LAYER_FILTER":
                # todo: if LAYER_FILTER implemented, add support for
                #  renaming layers
                logger.debug(
                    f'renaming layer "{old_name}" - document contains ' f"LAYER_FILTER"
                )
            elif entity_type == "LAYER_INDEX":
                # todo: if LAYER_INDEX implemented, add support for
                #  renaming layers
                logger.debug(
                    f'renaming layer "{old_name}" - document contains ' f"LAYER_INDEX"
                )

    def get_vp_overrides(self) -> LayerOverrides:
        """Returns the :class:`LayerOverrides` object for this layer."""
        return LayerOverrides(self)

    def register_resources(self, registry: xref.Registry) -> None:
        """Register required resources to the resource registry."""
        assert self.doc is not None, "LAYER entity must be assigned to a document"
        super().register_resources(registry)
        registry.add_linetype(self.dxf.linetype)
        registry.add_handle(self.dxf.get("material_handle"))
        # current plot style will be replaced by default plot style "Normal"

    def map_resources(self, clone: Self, mapping: xref.ResourceMapper) -> None:
        """Translate resources from self to the copied entity."""
        assert isinstance(clone, Layer)
        super().map_resources(clone, mapping)
        self.dxf.linetype = mapping.get_linetype(self.dxf.linetype)

        mapping.map_existing_handle(self, clone, "material_handle", optional=True)
        # remove handles pointing to the source document:
        clone.dxf.discard("plotstyle_handle")  # replaced by plot style "Normal"
        clone.dxf.discard("unknown1")

        # create required handles to resources in the target document
        clone.set_required_attributes()
        # todo: map layer overrides
        # remove layer overrides
        clone.discard_extension_dict()

    def audit(self, auditor: Auditor) -> None:
        super().audit(auditor)
        linetype = self.dxf.linetype
        if auditor.doc.linetypes.has_entry(linetype):
            return
        self.dxf.linetype = "Continuous"
        auditor.fixed_error(
            code=AuditError.UNDEFINED_LINETYPE,
            message=f"Replaced undefined linetype {linetype} in layer '{self.dxf.name}' by CONTINUOUS",
            dxf_entity=self,
            data=linetype,
        )


@dataclass
class OverrideAttributes:
    aci: int
    rgb: Optional[clr.RGB]
    transparency: float
    linetype: str
    lineweight: int


class LayerOverrides:
    """This object stores the layer attribute overridden in VIEWPORT entities,
    where each VIEWPORT can have individual layer attribute overrides.

    Layer attributes which can be overridden:

        - ACI color
        - true color (rgb)
        - linetype
        - lineweight
        - transparency

    """

    def __init__(self, layer: Layer):
        assert layer.doc is not None, "valid DXF document required"
        self._layer = layer
        self._overrides = load_layer_overrides(layer)

    def has_overrides(self, vp_handle: Optional[str] = None) -> bool:
        """Returns ``True`` if attribute overrides exist for the given
        :class:`Viewport` handle.
        Returns ``True`` if `any` attribute overrides exist if the given
        handle is ``None``.
        """
        if vp_handle is None:
            return bool(self._overrides)
        return vp_handle in self._overrides

    def commit(self) -> None:
        """Write :class:`Viewport` overrides back into the :class:`Layer` entity.
        Without a commit() all changes are lost!
        """
        store_layer_overrides(self._layer, self._overrides)

    def _acquire_overrides(self, vp_handle: str) -> OverrideAttributes:
        """Returns the OverrideAttributes() instance for `vp_handle`, creates a new
        OverrideAttributes() instance if none exist.
        """
        return self._overrides.setdefault(
            vp_handle,
            default_ovr_settings(self._layer),
        )

    def _get_overrides(self, vp_handle: str) -> OverrideAttributes:
        """Returns the overrides for `vp_handle`, returns the default layer
        settings if no Override() instance exist.
        """
        try:
            return self._overrides[vp_handle]
        except KeyError:
            return default_ovr_settings(self._layer)

    def set_color(self, vp_handle: str, value: int) -> None:
        """Override the :ref:`ACI`.

        Raises:
            ValueError: invalid color value
        """
        # BYBLOCK or BYLAYER is not valid a layer color
        if not is_valid_layer_color_index(value):
            raise ValueError(f"invalid ACI value: {value}")
        vp_overrides = self._acquire_overrides(vp_handle)
        vp_overrides.aci = value

    def get_color(self, vp_handle: str) -> int:
        """Returns the :ref:`ACI` override or the original layer value if no
        override exist.
        """
        vp_overrides = self._get_overrides(vp_handle)
        return vp_overrides.aci

    def set_rgb(self, vp_handle: str, value: Optional[clr.RGB]):
        """Set the RGB override as (red, gree, blue) tuple or ``None`` to remove
        the true color setting.

        Raises:
            ValueError: invalid RGB value

        """
        if value is not None and not validator.is_valid_rgb(value):
            raise ValueError(f"invalid RGB value: {value}")
        vp_overrides = self._acquire_overrides(vp_handle)
        vp_overrides.rgb = value

    def get_rgb(self, vp_handle: str) -> Optional[clr.RGB]:
        """Returns the RGB override or the original layer value if no
        override exist. Returns ``None`` if no true color value is set.
        """
        vp_overrides = self._get_overrides(vp_handle)
        return vp_overrides.rgb

    def set_transparency(self, vp_handle: str, value: float) -> None:
        """Set the transparency override. A transparency of 0.0 is opaque and
        1.0 is fully transparent.

        Raises:
            ValueError: invalid transparency value

        """
        if not (0.0 <= value <= 1.0):
            raise ValueError(
                f"invalid transparency: {value}, has to be in the range [0, 1]"
            )
        vp_overrides = self._acquire_overrides(vp_handle)
        vp_overrides.transparency = value

    def get_transparency(self, vp_handle: str) -> float:
        """Returns the transparency override or the original layer value if no
        override exist. Returns 0.0 for opaque and 1.0 for fully transparent.
        """
        vp_overrides = self._get_overrides(vp_handle)
        return vp_overrides.transparency

    def set_linetype(self, vp_handle: str, value: str) -> None:
        """Set the linetype override.

        Raises:
            ValueError: linetype without a LTYPE table entry
        """
        if value not in self._layer.doc.linetypes:  # type: ignore
            raise ValueError(
                f"invalid linetype: {value}, a linetype table entry is required"
            )
        vp_overrides = self._acquire_overrides(vp_handle)
        vp_overrides.linetype = value

    def get_linetype(self, vp_handle: str) -> str:
        """Returns the linetype override or the original layer value if no
        override exist.
        """
        vp_overrides = self._get_overrides(vp_handle)
        return vp_overrides.linetype

    def get_lineweight(self, vp_handle: str) -> int:
        """Returns the lineweight override or the original layer value if no
        override exist.
        """
        vp_overrides = self._get_overrides(vp_handle)
        return vp_overrides.lineweight

    def set_lineweight(self, vp_handle: str, value: int) -> None:
        """Set the lineweight override.

        Raises:
            ValueError: invalid lineweight value
        """
        if not is_valid_layer_lineweight(value):
            raise ValueError(
                f"invalid lineweight: {value}, a linetype table entry is required"
            )
        vp_overrides = self._acquire_overrides(vp_handle)
        vp_overrides.lineweight = value

    def discard(self, vp_handle: Optional[str] = None) -> None:
        """Discard all attribute overrides for the given :class:`Viewport`
        handle or for all :class:`Viewport` entities if the handle is ``None``.
        """
        if vp_handle is None:
            self._overrides.clear()
            return
        try:
            del self._overrides[vp_handle]
        except KeyError:
            pass


def default_ovr_settings(layer) -> OverrideAttributes:
    """Returns the default settings of the layer."""
    return OverrideAttributes(
        aci=layer.color,
        rgb=layer.rgb,
        transparency=layer.transparency,
        linetype=layer.dxf.linetype,
        lineweight=layer.dxf.lineweight,
    )


def is_layer_frozen_in_vp(layer, vp_handle) -> bool:
    """Returns ``True`` if layer is frozen in VIEWPORT defined by the vp_handle."""
    vp = cast("Viewport", layer.doc.entitydb.get(vp_handle))
    if vp is not None:
        return layer.dxf.name in vp.frozen_layers
    return False


def load_layer_overrides(layer: Layer) -> dict[str, OverrideAttributes]:
    """Load all VIEWPORT overrides from the layer extension dictionary."""

    def get_ovr(vp_handle: str):
        ovr = overrides.get(vp_handle)
        if ovr is None:
            ovr = default_ovr_settings(layer)
            overrides[vp_handle] = ovr
        return ovr

    def set_alpha(vp_handle: str, value: int):
        ovr = get_ovr(vp_handle)
        ovr.transparency = clr.transparency2float(value)

    def set_color(vp_handle: str, value: int):
        ovr = get_ovr(vp_handle)
        type_, data = clr.decode_raw_color(value)
        if type_ == clr.COLOR_TYPE_ACI:
            ovr.aci = data
        elif type_ == clr.COLOR_TYPE_RGB:
            ovr.rgb = data

    def set_ltype(vp_handle: str, lt_handle: str):
        ltype = entitydb.get(lt_handle)
        if ltype is not None:
            ovr = get_ovr(vp_handle)
            ovr.linetype = ltype.dxf.name

    def set_lw(vp_handle: str, value: int):
        ovr = get_ovr(vp_handle)
        ovr.lineweight = value

    def set_xdict_state():
        xdict = layer.get_extension_dict()
        for key, code, setter in [
            (const.OVR_ALPHA_KEY, const.OVR_ALPHA_CODE, set_alpha),
            (const.OVR_COLOR_KEY, const.OVR_COLOR_CODE, set_color),
            (const.OVR_LTYPE_KEY, const.OVR_LTYPE_CODE, set_ltype),
            (const.OVR_LW_KEY, const.OVR_LW_CODE, set_lw),
        ]:
            xrec = cast("XRecord", xdict.get(key))
            if xrec is not None:
                for vp_handle, value in _load_ovr_values(xrec, code):
                    setter(vp_handle, value)

    assert layer.doc is not None, "valid DXF document required"
    entitydb: EntityDB = layer.doc.entitydb
    assert entitydb is not None, "valid entity database required"

    overrides: dict[str, OverrideAttributes] = dict()
    if not layer.has_extension_dict:
        return overrides

    set_xdict_state()
    return overrides


def _load_ovr_values(xrec: XRecord, group_code):
    tags = xrec.tags
    handles = [value for code, value in tags.find_all(const.OVR_VP_HANDLE_CODE)]
    values = [value for code, value in tags.find_all(group_code)]
    return zip(handles, values)


def store_layer_overrides(
    layer: Layer, overrides: dict[str, OverrideAttributes]
) -> None:
    """Store all VIEWPORT overrides in the layer extension dictionary.
    Replaces all existing overrides!
    """
    from ezdxf.lldxf.types import DXFTag

    def get_xdict():
        if layer.has_extension_dict:
            return layer.get_extension_dict()
        else:
            return layer.new_extension_dict()

    def set_xdict_tags(key: str, tags: list[DXFTag]):
        from ezdxf.entities import XRecord

        xdict = get_xdict()
        xrec = xdict.get(key)
        if not isinstance(xrec, XRecord) and xrec is not None:
            logger.debug(
                f"Found entity {str(xrec)} as override storage in {str(layer)} "
                f"but expected XRECORD"
            )
            xrec = None
        if xrec is None:
            xrec = xdict.add_xrecord(key)
            xrec.dxf.cloning = 1
        xrec.reset(tags)

    def del_xdict_tags(key: str):
        if not layer.has_extension_dict:
            return
        xdict = layer.get_extension_dict()
        xrec = xdict.get(key)
        if xrec is not None:
            xrec.destroy()
            xdict.discard(key)

    def make_tags(data: list[tuple[Any, str]], name: str, code: int) -> list[DXFTag]:
        tags: list[DXFTag] = []
        for value, vp_handle in data:
            tags.extend(
                [
                    DXFTag(102, name),
                    DXFTag(const.OVR_VP_HANDLE_CODE, vp_handle),
                    DXFTag(code, value),
                    DXFTag(102, "}"),
                ]
            )
        return tags

    def collect_alphas():
        for vp_handle, ovr in vp_exist.items():
            if ovr.transparency != default.transparency:
                yield clr.float2transparency(ovr.transparency), vp_handle

    def collect_colors():
        for vp_handle, ovr in vp_exist.items():
            if ovr.aci != default.aci or ovr.rgb != default.rgb:
                if ovr.rgb is None:
                    raw_color = clr.encode_raw_color(ovr.aci)
                else:
                    raw_color = clr.encode_raw_color(ovr.rgb)
                yield raw_color, vp_handle

    def collect_linetypes():
        for vp_handle, ovr in vp_exist.items():
            if ovr.linetype != default.linetype:
                ltype = layer.doc.linetypes.get(ovr.linetype)
                if ltype is not None:
                    yield ltype.dxf.handle, vp_handle

    def collect_lineweights():
        for vp_handle, ovr in vp_exist.items():
            if ovr.lineweight != default.lineweight:
                yield ovr.lineweight, vp_handle

    assert layer.doc is not None, "valid DXF document required"
    entitydb = layer.doc.entitydb
    vp_exist = {
        vp_handle: ovr
        for vp_handle, ovr in overrides.items()
        if (vp_handle in entitydb) and entitydb[vp_handle].is_alive
    }
    default = default_ovr_settings(layer)
    alphas = list(collect_alphas())
    if alphas:
        tags = make_tags(alphas, const.OVR_APP_ALPHA, const.OVR_ALPHA_CODE)
        set_xdict_tags(const.OVR_ALPHA_KEY, tags)
    else:
        del_xdict_tags(const.OVR_ALPHA_KEY)

    colors = list(collect_colors())
    if colors:
        tags = make_tags(colors, const.OVR_APP_COLOR, const.OVR_COLOR_CODE)
        set_xdict_tags(const.OVR_COLOR_KEY, tags)
    else:
        del_xdict_tags(const.OVR_COLOR_KEY)

    linetypes = list(collect_linetypes())
    if linetypes:
        tags = make_tags(linetypes, const.OVR_APP_LTYPE, const.OVR_LTYPE_CODE)
        set_xdict_tags(const.OVR_LTYPE_KEY, tags)
    else:
        del_xdict_tags(const.OVR_LTYPE_KEY)

    lineweights = list(collect_lineweights())
    if lineweights:
        tags = make_tags(lineweights, const.OVR_APP_LW, const.OVR_LW_CODE)
        set_xdict_tags(const.OVR_LW_KEY, tags)
    else:
        del_xdict_tags(const.OVR_LW_KEY)
