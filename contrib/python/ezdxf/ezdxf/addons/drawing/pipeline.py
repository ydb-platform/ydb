#  Copyright (c) 2023-2024, Manfred Moitzi
#  License: MIT License
from __future__ import annotations
from typing import (
    Sequence,
    Optional,
    Iterable,
    Tuple,
    Iterator,
    Callable,
)
from typing_extensions import TypeAlias
import abc

import numpy as np
import PIL.Image
import PIL.ImageDraw
import PIL.ImageOps


from ezdxf.colors import RGB
import ezdxf.bbox

from ezdxf.fonts import fonts
from ezdxf.math import Vec2, Matrix44, BoundingBox2d, AnyVec
from ezdxf.path import make_path, Path
from ezdxf.render import linetypes
from ezdxf.entities import DXFGraphic, Viewport
from ezdxf.tools.text import replace_non_printable_characters
from ezdxf.tools.clipping_portal import (
    ClippingPortal,
    ClippingShape,
    find_best_clipping_shape,
)
from ezdxf.layouts import Layout
from .backend import BackendInterface, BkPath2d, BkPoints2d, ImageData
from .config import LinePolicy, TextPolicy, ColorPolicy, Configuration
from .properties import BackendProperties, Filling
from .properties import Properties, RenderContext
from .type_hints import Color
from .unified_text_renderer import UnifiedTextRenderer

PatternKey: TypeAlias = Tuple[str, float]
DrawEntitiesCallback: TypeAlias = Callable[[RenderContext, Iterable[DXFGraphic]], None]

__all__ = ["AbstractPipeline", "RenderPipeline2d"]


class AbstractPipeline(abc.ABC):
    """This drawing pipeline separates the frontend from the backend and implements
    these features:

    - automatically linetype rendering
    - font rendering
    - VIEWPORT rendering
    - foreground color mapping according Frontend.config.color_policy

    The pipeline is organized as concatenated render stages.

    """

    text_engine = UnifiedTextRenderer()
    default_font_face = fonts.FontFace()
    draw_entities: DrawEntitiesCallback

    @abc.abstractmethod
    def set_draw_entities_callback(self, callback: DrawEntitiesCallback) -> None: ...

    @abc.abstractmethod
    def set_config(self, config: Configuration) -> None: ...

    @abc.abstractmethod
    def set_current_entity_handle(self, handle: str) -> None: ...

    @abc.abstractmethod
    def push_clipping_shape(
        self, shape: ClippingShape, transform: Matrix44 | None
    ) -> None: ...

    @abc.abstractmethod
    def pop_clipping_shape(self) -> None: ...

    @abc.abstractmethod
    def draw_viewport(
        self,
        vp: Viewport,
        layout_ctx: RenderContext,
        bbox_cache: Optional[ezdxf.bbox.Cache] = None,
    ) -> None:
        """Draw the content of the given viewport current viewport."""
        ...

    @abc.abstractmethod
    def draw_point(self, pos: AnyVec, properties: Properties) -> None: ...

    @abc.abstractmethod
    def draw_line(self, start: AnyVec, end: AnyVec, properties: Properties): ...

    @abc.abstractmethod
    def draw_solid_lines(
        self, lines: Iterable[tuple[AnyVec, AnyVec]], properties: Properties
    ) -> None: ...

    @abc.abstractmethod
    def draw_path(self, path: Path, properties: Properties): ...

    @abc.abstractmethod
    def draw_filled_paths(
        self,
        paths: Iterable[Path],
        properties: Properties,
    ) -> None: ...

    @abc.abstractmethod
    def draw_filled_polygon(
        self, points: Iterable[AnyVec], properties: Properties
    ) -> None: ...

    @abc.abstractmethod
    def draw_text(
        self,
        text: str,
        transform: Matrix44,
        properties: Properties,
        cap_height: float,
        dxftype: str = "TEXT",
    ) -> None: ...

    @abc.abstractmethod
    def draw_image(self, image_data: ImageData, properties: Properties) -> None: ...

    @abc.abstractmethod
    def finalize(self) -> None: ...

    @abc.abstractmethod
    def set_background(self, color: Color) -> None: ...

    @abc.abstractmethod
    def enter_entity(self, entity: DXFGraphic, properties: Properties) -> None:
        # gets the full DXF properties information
        ...

    @abc.abstractmethod
    def exit_entity(self, entity: DXFGraphic) -> None: ...


class RenderStage2d(abc.ABC):
    next_stage: RenderStage2d

    def set_config(self, config: Configuration) -> None:
        pass

    @abc.abstractmethod
    def draw_point(self, pos: Vec2, properties: Properties) -> None: ...

    @abc.abstractmethod
    def draw_line(self, start: Vec2, end: Vec2, properties: Properties): ...

    @abc.abstractmethod
    def draw_solid_lines(
        self, lines: list[tuple[Vec2, Vec2]], properties: Properties
    ) -> None: ...

    @abc.abstractmethod
    def draw_path(self, path: BkPath2d, properties: Properties): ...

    @abc.abstractmethod
    def draw_filled_paths(
        self,
        paths: list[BkPath2d],
        properties: Properties,
    ) -> None: ...

    @abc.abstractmethod
    def draw_filled_polygon(
        self, points: BkPoints2d, properties: Properties
    ) -> None: ...

    @abc.abstractmethod
    def draw_image(self, image_data: ImageData, properties: Properties) -> None: ...


class RenderPipeline2d(AbstractPipeline):
    """Render pipeline for 2D backends."""

    def __init__(self, backend: BackendInterface):
        self.backend = backend
        self.config = Configuration()
        try:  # request default font face
            self.default_font_face = fonts.font_manager.get_font_face("")
        except fonts.FontNotFoundError:  # no default font found
            # last resort MonospaceFont which renders only "tofu"
            pass
        self.clipping_portal = ClippingPortal()
        self.current_vp_scale = 1.0
        self._current_entity_handle: str = ""
        self._color_mapping: dict[str, str] = dict()
        self._pipeline = self.build_render_pipeline()

    def build_render_pipeline(self) -> RenderStage2d:
        backend_stage = BackendStage2d(
            self.backend, converter=self.get_backend_properties
        )
        linetype_stage = LinetypeStage2d(
            self.config,
            get_ltype_scale=self.get_vp_ltype_scale,
            next_stage=backend_stage,
        )
        clipping_stage = ClippingStage2d(
            self.config, self.clipping_portal, next_stage=linetype_stage
        )
        return clipping_stage

    def get_vp_ltype_scale(self) -> float:
        """The linetype pattern should look the same in all viewports
        regardless of the viewport scale.
        """
        return 1.0 / max(self.current_vp_scale, 0.0001)  # max out at 1:10000

    def get_backend_properties(self, properties: Properties) -> BackendProperties:
        try:
            color = self._color_mapping[properties.color]
        except KeyError:
            color = apply_color_policy(
                properties.color, self.config.color_policy, self.config.custom_fg_color
            )
            self._color_mapping[properties.color] = color
        return BackendProperties(
            color,
            properties.lineweight,
            properties.layer,
            properties.pen,
            self._current_entity_handle,
        )

    def set_draw_entities_callback(self, callback: DrawEntitiesCallback) -> None:
        self.draw_entities = callback

    def set_config(self, config: Configuration) -> None:
        self.backend.configure(config)
        self.config = config
        stage = self._pipeline
        while True:
            stage.set_config(config)
            if not hasattr(stage, "next_stage"):  # BackendStage2d
                return
            stage = stage.next_stage

    def set_current_entity_handle(self, handle: str) -> None:
        assert handle is not None
        self._current_entity_handle = handle

    def push_clipping_shape(
        self, shape: ClippingShape, transform: Matrix44 | None
    ) -> None:
        self.clipping_portal.push(shape, transform)

    def pop_clipping_shape(self) -> None:
        self.clipping_portal.pop()

    def draw_viewport(
        self,
        vp: Viewport,
        layout_ctx: RenderContext,
        bbox_cache: Optional[ezdxf.bbox.Cache] = None,
    ) -> None:
        """Draw the content of the given viewport current viewport."""
        if vp.doc is None:
            return
        try:
            msp_limits = vp.get_modelspace_limits()
        except ValueError:  # modelspace limits not detectable
            return
        if self.enter_viewport(vp):
            self.draw_entities(
                layout_ctx.from_viewport(vp),
                filter_vp_entities(vp.doc.modelspace(), msp_limits, bbox_cache),
            )
            self.exit_viewport()

    def enter_viewport(self, vp: Viewport) -> bool:
        """Set current viewport, returns ``True`` for valid viewports."""
        self.current_vp_scale = vp.get_scale()
        m = vp.get_transformation_matrix()
        clipping_path = make_path(vp)
        if len(clipping_path):
            vertices = clipping_path.control_vertices()
            if clipping_path.has_curves:
                layout = vp.get_layout()
                if isinstance(layout, Layout):
                    # plot paper units:
                    #   0: inches, max sagitta = 1/254 = 0.1 mm
                    #   1: millimeters, max sagitta = 0.1 mm
                    #   2: pixels, max sagitta = 0.1 pixel
                    units = layout.dxf.get("plot_paper_units", 1)
                    max_sagitta = 1.0 / 254.0 if units == 0 else 0.1
                    vertices = list(clipping_path.flattening(max_sagitta))
            clipping_shape = find_best_clipping_shape(vertices)
            self.clipping_portal.push(clipping_shape, m)
            return True
        return False

    def exit_viewport(self):
        self.clipping_portal.pop()
        # Reset viewport scaling: viewports cannot be nested!
        self.current_vp_scale = 1.0

    def draw_text(
        self,
        text: str,
        transform: Matrix44,
        properties: Properties,
        cap_height: float,
        dxftype: str = "TEXT",
    ) -> None:
        """Render text as filled paths."""
        text_policy = self.config.text_policy
        pipeline = self._pipeline

        if not text.strip() or text_policy == TextPolicy.IGNORE:
            return  # no point rendering empty strings
        text = prepare_string_for_rendering(text, dxftype)
        font_face = properties.font
        if font_face is None:
            font_face = self.default_font_face

        try:
            glyph_paths = self.text_engine.get_text_glyph_paths(
                text, font_face, cap_height
            )
        except (RuntimeError, ValueError):
            return
        for p in glyph_paths:
            p.transform_inplace(transform)
        transformed_paths: list[BkPath2d] = glyph_paths

        points: list[Vec2]
        if text_policy == TextPolicy.REPLACE_RECT:
            points = []
            for p in transformed_paths:
                points.extend(p.extents())
            if len(points) < 2:
                return
            rect = BkPath2d.from_vertices(BoundingBox2d(points).rect_vertices())
            pipeline.draw_path(rect, properties)
            return
        if text_policy == TextPolicy.REPLACE_FILL:
            points = []
            for p in transformed_paths:
                points.extend(p.extents())
            if len(points) < 2:
                return
            polygon = BkPoints2d(BoundingBox2d(points).rect_vertices())
            if properties.filling is None:
                properties.filling = Filling()
            pipeline.draw_filled_polygon(polygon, properties)
            return

        if (
            self.text_engine.is_stroke_font(font_face)
            or text_policy == TextPolicy.OUTLINE
        ):
            for text_path in transformed_paths:
                pipeline.draw_path(text_path, properties)
            return

        if properties.filling is None:
            properties.filling = Filling()
        pipeline.draw_filled_paths(transformed_paths, properties)

    def finalize(self) -> None:
        self.backend.finalize()

    def set_background(self, color: Color) -> None:
        self.backend.set_background(color)

    def enter_entity(self, entity: DXFGraphic, properties: Properties) -> None:
        self.backend.enter_entity(entity, properties)

    def exit_entity(self, entity: DXFGraphic) -> None:
        self.backend.exit_entity(entity)

    # Enter render pipeline:
    def draw_point(self, pos: AnyVec, properties: Properties) -> None:
        self._pipeline.draw_point(Vec2(pos), properties)

    def draw_line(self, start: AnyVec, end: AnyVec, properties: Properties):
        self._pipeline.draw_line(Vec2(start), Vec2(end), properties)

    def draw_solid_lines(
        self, lines: Iterable[tuple[AnyVec, AnyVec]], properties: Properties
    ) -> None:
        self._pipeline.draw_solid_lines(
            [(Vec2(s), Vec2(e)) for s, e in lines], properties
        )

    def draw_path(self, path: Path, properties: Properties):
        self._pipeline.draw_path(BkPath2d(path), properties)

    def draw_filled_paths(
        self,
        paths: Iterable[Path],
        properties: Properties,
    ) -> None:
        self._pipeline.draw_filled_paths(list(map(BkPath2d, paths)), properties)

    def draw_filled_polygon(
        self, points: Iterable[AnyVec], properties: Properties
    ) -> None:
        self._pipeline.draw_filled_polygon(BkPoints2d(points), properties)

    def draw_image(self, image_data: ImageData, properties: Properties) -> None:
        self._pipeline.draw_image(image_data, properties)


class ClippingStage2d(RenderStage2d):
    def __init__(
        self,
        config: Configuration,
        clipping_portal: ClippingPortal,
        next_stage: RenderStage2d,
    ):
        self.clipping_portal = clipping_portal
        self.config = config
        self.next_stage = next_stage

    def set_config(self, config: Configuration) -> None:
        self.config = config

    def draw_point(self, pos: Vec2, properties: Properties) -> None:
        if self.clipping_portal.is_active:
            pos = self.clipping_portal.clip_point(pos)
            if pos is None:
                return
        self.next_stage.draw_point(pos, properties)

    def draw_line(self, start: Vec2, end: Vec2, properties: Properties):
        next_stage = self.next_stage
        clipping_portal = self.clipping_portal

        if clipping_portal.is_active:
            for segment in clipping_portal.clip_line(start, end):
                next_stage.draw_line(segment[0], segment[1], properties)
            return
        next_stage.draw_line(start, end, properties)

    def draw_solid_lines(
        self, lines: list[tuple[Vec2, Vec2]], properties: Properties
    ) -> None:
        clipping_portal = self.clipping_portal

        if clipping_portal.is_active:
            cropped_lines: list[tuple[Vec2, Vec2]] = []
            for start, end in lines:
                cropped_lines.extend(clipping_portal.clip_line(start, end))
            lines = cropped_lines
        self.next_stage.draw_solid_lines(lines, properties)

    def draw_path(self, path: BkPath2d, properties: Properties):
        clipping_portal = self.clipping_portal
        next_stage = self.next_stage
        max_sagitta = self.config.max_flattening_distance

        if clipping_portal.is_active:
            for clipped_path in clipping_portal.clip_paths([path], max_sagitta):
                next_stage.draw_path(clipped_path, properties)
            return
        next_stage.draw_path(path, properties)

    def draw_filled_paths(
        self,
        paths: list[BkPath2d],
        properties: Properties,
    ) -> None:
        clipping_portal = self.clipping_portal
        max_sagitta = self.config.max_flattening_distance

        if clipping_portal.is_active:
            paths = clipping_portal.clip_filled_paths(paths, max_sagitta)
        if len(paths) == 0:
            return
        self.next_stage.draw_filled_paths(paths, properties)

    def draw_filled_polygon(self, points: BkPoints2d, properties: Properties) -> None:
        clipping_portal = self.clipping_portal
        next_stage = self.next_stage

        if clipping_portal.is_active:
            for points in clipping_portal.clip_polygon(points):
                if len(points) > 0:
                    next_stage.draw_filled_polygon(points, properties)
            return

        if len(points) > 0:
            next_stage.draw_filled_polygon(points, properties)

    def draw_image(self, image_data: ImageData, properties: Properties) -> None:
        # the outer bounds contain the visible parts of the image for the
        # clip mode "remove inside"
        outer_bounds: list[BkPoints2d] = []
        clipping_portal = self.clipping_portal

        if not clipping_portal.is_active:
            self._draw_image(image_data, outer_bounds, properties)
            return

        # the pixel boundary path can be split into multiple paths
        transform = image_data.flip_matrix() * image_data.transform
        pixel_boundary_path = image_data.pixel_boundary_path
        clipping_paths = _clip_image_polygon(
            clipping_portal, pixel_boundary_path, transform
        )
        if not image_data.remove_outside:
            # remove inside:
            #  detect the visible parts of the image which are not removed by
            #  clipping through viewports or block references
            width, height = image_data.image_size()
            outer_boundary = BkPoints2d(
                Vec2.generate([(0, 0), (width, 0), (width, height), (0, height)])
            )
            outer_bounds = _clip_image_polygon(
                clipping_portal, outer_boundary, transform
            )
        image_data.transform = clipping_portal.transform_matrix(image_data.transform)
        if len(clipping_paths) == 1:
            new_clipping_path = clipping_paths[0]
            if new_clipping_path is not image_data.pixel_boundary_path:
                image_data.pixel_boundary_path = new_clipping_path
                # forced clipping triggered by viewport- or block reference clipping:
                image_data.use_clipping_boundary = True
            self._draw_image(image_data, outer_bounds, properties)
        else:
            for clipping_path in clipping_paths:
                # when clipping path is split into multiple parts:
                #  copy image for each part, not efficient but works
                #  this should be a rare usecase so optimization is not required
                self._draw_image(
                    ImageData(
                        image=image_data.image.copy(),
                        transform=image_data.transform,
                        pixel_boundary_path=clipping_path,
                        use_clipping_boundary=True,
                    ),
                    outer_bounds,
                    properties,
                )

    def _draw_image(
        self,
        image_data: ImageData,
        outer_bounds: list[BkPoints2d],
        properties: Properties,
    ) -> None:
        if image_data.use_clipping_boundary:
            _mask_image(image_data, outer_bounds)
        self.next_stage.draw_image(image_data, properties)


class LinetypeStage2d(RenderStage2d):
    def __init__(
        self,
        config: Configuration,
        get_ltype_scale: Callable[[], float],
        next_stage: RenderStage2d,
    ):
        self.config = config
        self.solid_lines_only = False
        self.next_stage = next_stage
        self.get_ltype_scale = get_ltype_scale
        self.pattern_cache: dict[PatternKey, Sequence[float]] = dict()
        self.set_config(config)

    def set_config(self, config: Configuration) -> None:
        self.config = config
        self.solid_lines_only = config.line_policy == LinePolicy.SOLID

    def pattern(self, properties: Properties) -> Sequence[float]:
        """Returns simplified linetype tuple: on-off sequence"""
        if self.solid_lines_only:
            scale = 0.0
        else:
            scale = properties.linetype_scale * self.get_ltype_scale()

        key: PatternKey = (properties.linetype_name, scale)
        pattern_ = self.pattern_cache.get(key)
        if pattern_ is None:
            pattern_ = self._create_pattern(properties, scale)
            self.pattern_cache[key] = pattern_
        return pattern_

    def _create_pattern(self, properties: Properties, scale: float) -> Sequence[float]:
        if len(properties.linetype_pattern) < 2:
            # Do not return None -> None indicates: "not cached"
            return tuple()

        min_dash_length = self.config.min_dash_length * self.get_ltype_scale()
        pattern = [max(e * scale, min_dash_length) for e in properties.linetype_pattern]
        if len(pattern) % 2:
            pattern.pop()
        return pattern

    def draw_point(self, pos: Vec2, properties: Properties) -> None:
        self.next_stage.draw_point(pos, properties)

    def draw_line(self, start: Vec2, end: Vec2, properties: Properties):
        s = Vec2(start)
        e = Vec2(end)
        next_stage = self.next_stage

        if self.solid_lines_only or len(properties.linetype_pattern) < 2:  # CONTINUOUS
            next_stage.draw_line(s, e, properties)
            return

        renderer = linetypes.LineTypeRenderer(self.pattern(properties))
        next_stage.draw_solid_lines(
            [(s, e) for s, e in renderer.line_segment(s, e)],
            properties,
        )

    def draw_solid_lines(
        self, lines: list[tuple[Vec2, Vec2]], properties: Properties
    ) -> None:
        self.next_stage.draw_solid_lines(lines, properties)

    def draw_path(self, path: BkPath2d, properties: Properties):
        next_stage = self.next_stage

        if self.solid_lines_only or len(properties.linetype_pattern) < 2:  # CONTINUOUS
            next_stage.draw_path(path, properties)
            return

        renderer = linetypes.LineTypeRenderer(self.pattern(properties))
        vertices = path.flattening(self.config.max_flattening_distance, segments=16)
        next_stage.draw_solid_lines(
            [(Vec2(s), Vec2(e)) for s, e in renderer.line_segments(vertices)],
            properties,
        )

    def draw_filled_paths(
        self,
        paths: list[BkPath2d],
        properties: Properties,
    ) -> None:
        self.next_stage.draw_filled_paths(paths, properties)

    def draw_filled_polygon(self, points: BkPoints2d, properties: Properties) -> None:
        self.next_stage.draw_filled_polygon(points, properties)

    def draw_image(self, image_data: ImageData, properties: Properties) -> None:
        self.next_stage.draw_image(image_data, properties)


class BackendStage2d(RenderStage2d):
    """Send data to the output backend."""

    def __init__(
        self,
        backend: BackendInterface,
        converter: Callable[[Properties], BackendProperties],
    ):
        self.backend = backend
        self.converter = converter
        assert not hasattr(self, "next_stage"), "has to be the last render stage"

    def draw_point(self, pos: Vec2, properties: Properties) -> None:
        self.backend.draw_point(pos, self.converter(properties))

    def draw_line(self, start: Vec2, end: Vec2, properties: Properties):
        self.backend.draw_line(start, end, self.converter(properties))

    def draw_solid_lines(
        self, lines: list[tuple[Vec2, Vec2]], properties: Properties
    ) -> None:
        self.backend.draw_solid_lines(lines, self.converter(properties))

    def draw_path(self, path: BkPath2d, properties: Properties):
        self.backend.draw_path(path, self.converter(properties))

    def draw_filled_paths(
        self,
        paths: list[BkPath2d],
        properties: Properties,
    ) -> None:
        self.backend.draw_filled_paths(paths, self.converter(properties))

    def draw_filled_polygon(self, points: BkPoints2d, properties: Properties) -> None:
        self.backend.draw_filled_polygon(points, self.converter(properties))

    def draw_image(self, image_data: ImageData, properties: Properties) -> None:
        self.backend.draw_image(image_data, self.converter(properties))


def _mask_image(image_data: ImageData, outer_bounds: list[BkPoints2d]) -> None:
    """Mask away the clipped parts of the image. The argument `outer_bounds` is only
    used for clip mode "remove_inside". The outer bounds can be composed of multiple
    parts. If `outer_bounds` is empty the image has no removed parts and is fully
    visible before applying the image clipping path.

    Args:
        image_data:
            image_data.pixel_boundary: path contains the image clipping path
            image_data.remove_outside: defines the clipping mode (inside/outside)
        outer_bounds: countain the parts of the image which are __not__ removed by
            clipping through viewports or clipped block references
            e.g. an image without any removed parts has the outer bounds
            [(0, 0) (width, 0), (width, height), (0, height)]

    """
    clip_polygon = [(p.x, p.y) for p in image_data.pixel_boundary_path.vertices()]
    # create an empty image
    clipping_image = PIL.Image.new("L", image_data.image_size(), 0)
    # paint in the clipping path
    PIL.ImageDraw.ImageDraw(clipping_image).polygon(
        clip_polygon, outline=None, width=0, fill=1
    )
    clipping_mask = np.asarray(clipping_image)

    if not image_data.remove_outside:  # clip mode "remove_inside"
        if outer_bounds:
            # create a new empty image
            visible_image = PIL.Image.new("L", image_data.image_size(), 0)
            # paint in parts of the image which are still visible
            for boundary in outer_bounds:
                clip_polygon = [(p.x, p.y) for p in boundary.vertices()]
                PIL.ImageDraw.ImageDraw(visible_image).polygon(
                    clip_polygon, outline=None, width=0, fill=1
                )
            # remove the clipping path
            clipping_mask = np.asarray(visible_image) - clipping_mask
        else:
            # create mask for fully visible image
            fully_visible_image_mask = np.full(
                clipping_mask.shape, fill_value=1, dtype=clipping_mask.dtype
            )
            # remove the clipping path
            clipping_mask = fully_visible_image_mask - clipping_mask
    image_data.image[:, :, 3] *= clipping_mask


def _clip_image_polygon(
    clipping_portal: ClippingPortal, polygon_px: BkPoints2d, m: Matrix44
) -> list[BkPoints2d]:
    original = [polygon_px]

    # inverse matrix includes the transformation applied by the clipping portal
    inverse = clipping_portal.transform_matrix(m)
    try:
        inverse.inverse()
    except ZeroDivisionError:
        # inverse transformation from WCS to pixel coordinates is not possible
        return original

    # transform image coordinates to WCS coordinates
    polygon = polygon_px.clone()
    polygon.transform_inplace(m)

    clipped_polygons = clipping_portal.clip_polygon(polygon)
    if (len(clipped_polygons) == 1) and (clipped_polygons[0] is polygon):
        # this shows the caller that the image boundary path wasn't clipped
        return original
    # transform WCS coordinates to image coordinates
    for polygon in clipped_polygons:
        polygon.transform_inplace(inverse)
    return clipped_polygons  # in image coordinates!


def invert_color(color: Color) -> Color:
    r, g, b = RGB.from_hex(color)
    return RGB(255 - r, 255 - g, 255 - b).to_hex()


def swap_bw(color: str) -> Color:
    color = color.lower()
    if color == "#000000":
        return "#ffffff"
    if color == "#ffffff":
        return "#000000"
    return color


def color_to_monochrome(color: Color, scale: float = 1.0, offset: float = 0.0) -> Color:
    lum = RGB.from_hex(color).luminance * scale + offset
    if lum < 0.0:
        lum = 0.0
    elif lum > 1.0:
        lum = 1.0
    gray = round(lum * 255)
    return RGB(gray, gray, gray).to_hex()


def apply_color_policy(color: Color, policy: ColorPolicy, custom_color: Color) -> Color:
    alpha = color[7:9]
    color = color[:7]
    if policy == ColorPolicy.COLOR_SWAP_BW:
        color = swap_bw(color)
    elif policy == ColorPolicy.COLOR_NEGATIVE:
        color = invert_color(color)
    elif policy == ColorPolicy.MONOCHROME_DARK_BG:  # [0.3, 1.0]
        color = color_to_monochrome(color, scale=0.7, offset=0.3)
    elif policy == ColorPolicy.MONOCHROME_LIGHT_BG:  # [0.0, 0.7]
        color = color_to_monochrome(color, scale=0.7, offset=0.0)
    elif policy == ColorPolicy.MONOCHROME:  # [0.0, 1.0]
        color = color_to_monochrome(color)
    elif policy == ColorPolicy.BLACK:
        color = "#000000"
    elif policy == ColorPolicy.WHITE:
        color = "#ffffff"
    elif policy == ColorPolicy.CUSTOM:
        fg = custom_color
        color = fg[:7]
        alpha = fg[7:9]
    return color + alpha


def filter_vp_entities(
    msp: Layout,
    limits: Sequence[float],
    bbox_cache: Optional[ezdxf.bbox.Cache] = None,
) -> Iterator[DXFGraphic]:
    """Yields all DXF entities that need to be processed by the given viewport
    `limits`. The entities may be partially of even complete outside the viewport.
    By passing the bounding box cache of the modelspace entities,
    the function can filter entities outside the viewport to speed up rendering
    time.

    There are two processing modes for the `bbox_cache`:

        1. The `bbox_cache` is``None``: all entities must be processed,
           pass through mode
        2. If the `bbox_cache` is given but does not contain an entity,
           the bounding box is computed and added to the cache.
           Even passing in an empty cache can speed up rendering time when
           multiple viewports need to be processed.

    Args:
        msp: modelspace layout
        limits: modelspace limits of the viewport, as tuple (min_x, min_y, max_x, max_y)
        bbox_cache: the bounding box cache of the modelspace entities

    """

    # WARNING: this works only with top-view viewports
    # The current state of the drawing add-on supports only top-view viewports!
    def is_visible(e):
        entity_bbox = bbox_cache.get(e)
        if entity_bbox is None:
            # compute and add bounding box
            entity_bbox = ezdxf.bbox.extents((e,), fast=True, cache=bbox_cache)
        if not entity_bbox.has_data:
            return True
        # Check for separating axis:
        if min_x >= entity_bbox.extmax.x:
            return False
        if max_x <= entity_bbox.extmin.x:
            return False
        if min_y >= entity_bbox.extmax.y:
            return False
        if max_y <= entity_bbox.extmin.y:
            return False
        return True

    if bbox_cache is None:  # pass through all entities
        yield from msp
        return

    min_x, min_y, max_x, max_y = limits
    if not bbox_cache.has_data:
        # fill cache at once
        ezdxf.bbox.extents(msp, fast=True, cache=bbox_cache)

    for entity in msp:
        if is_visible(entity):
            yield entity


def prepare_string_for_rendering(text: str, dxftype: str) -> str:
    assert "\n" not in text, "not a single line of text"
    if dxftype in {"TEXT", "ATTRIB", "ATTDEF"}:
        text = replace_non_printable_characters(text, replacement="?")
        text = text.replace("\t", "?")
    elif dxftype == "MTEXT":
        text = replace_non_printable_characters(text, replacement="▯")
        text = text.replace("\t", "        ")
    else:
        raise TypeError(dxftype)
    return text
