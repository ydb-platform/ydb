import itertools
from collections import defaultdict
from html import escape

import numpy as np
import param
from bokeh.models import Arrow, BoxAnnotation, NormalHead, Slope, Span, TeeHead
from bokeh.transform import dodge
from panel.models import HTML

from ...core.util import datetime_types, dimension_sanitizer
from ...element import HLine, HLines, HSpans, VLine, VLines, VSpan, VSpans
from ..plot import GenericElementPlot
from .element import AnnotationPlot, ColorbarPlot, CompositeElementPlot, ElementPlot
from .plot import BokehPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import (
    background_properties,
    base_properties,
    border_properties,
    fill_properties,
    line_properties,
    text_properties,
)
from .util import BOKEH_GE_3_2_0, BOKEH_GE_3_9_0, date_to_integer

arrow_start = {'<->': NormalHead, '<|-|>': NormalHead}
arrow_end = {'->': NormalHead, '-[': TeeHead, '-|>': NormalHead,
                '-': None}


class _SyntheticAnnotationPlot(ColorbarPlot):

    apply_ranges = param.Boolean(default=True, doc="""
        Whether to include the annotation in axis range calculations.""")

    style_opts = [*line_properties, 'level', 'visible']
    _allow_implicit_categories = False

    def __init__(self, element, **kwargs):
        if not BOKEH_GE_3_2_0:
            name = type(getattr(element, "last", element)).__name__
            msg = f'{name} element requires Bokeh >=3.2'
            raise ImportError(msg)
        super().__init__(element, **kwargs)

    def _get_axis_dims(self, element):
        if isinstance(element, (HLines, HSpans)):
            return None, element.kdims[0], None
        return element.kdims[0], None, None

    def _init_glyph(self, plot, mapping, properties):
        self._plot_methods = {"single": self._methods[self.invert_axes]}
        return super()._init_glyph(plot, mapping, properties)

    def get_data(self, element, ranges, style):
        data = element.columns(element.kdims)
        self._get_hover_data(data, element)
        default = self._element_default[self.invert_axes].kdims
        mapping = {str(d): str(k) for d, k in zip(default, element.kdims, strict=None)}
        return data, mapping, style

    def initialize_plot(self, ranges=None, plot=None, plots=None, source=None):
        figure = super().initialize_plot(ranges=ranges, plot=plot, plots=plots, source=source)
        # Only force labels if no other ranges are set
        if self.overlaid and set(itertools.chain.from_iterable(ranges)) - {"HSpans", "VSpans", "VLines", "HLines"}:
            return figure
        labels = [self.xlabel or "x", self.ylabel or "y"]
        labels = labels[::-1] if self.invert_axes else labels
        for ax, label in zip(figure.axis, labels, strict=None):
            ax.axis_label = label
        return figure

    def get_extents(self, element, ranges=None, range_type='combined', **kwargs):
        import pandas as pd

        extents = super().get_extents(element, ranges, range_type)
        if isinstance(element, HLines):
            extents = np.nan, extents[0], np.nan, extents[2]
        elif isinstance(element, VLines):
            extents = extents[0], np.nan, extents[2], np.nan
        elif isinstance(element, HSpans):
            extents = pd.array(extents)
            extents = np.nan, extents[:2].min(), np.nan, extents[2:].max()
        elif isinstance(element, VSpans):
            extents = pd.array(extents)
            extents = extents[:2].min(), np.nan, extents[2:].max(), np.nan
        return extents

class HLinesAnnotationPlot(_SyntheticAnnotationPlot):

    # If invert_axes is False we use the first method,
    # and if True the second as _plot_methods(single=...)
    _methods = ('hspan', 'vspan')
    _element_default = (HLines, VLines)


class VLinesAnnotationPlot(_SyntheticAnnotationPlot):

    _methods = ('vspan', 'hspan')
    _element_default = (VLines, HLines)


class HSpansAnnotationPlot(_SyntheticAnnotationPlot):

    _methods = ('hstrip', 'vstrip')
    _element_default = (HSpans, VSpans)
    style_opts = [*fill_properties, *line_properties, 'level', 'visible']


class VSpansAnnotationPlot(_SyntheticAnnotationPlot):

    _methods = ('vstrip', 'hstrip')
    _element_default = (VSpans, HSpans)
    style_opts = [*fill_properties, *line_properties, 'level', 'visible']


class TextPlot(ElementPlot, AnnotationPlot):

    style_opts = (text_properties + background_properties
                  + border_properties + ['color', 'angle', 'visible'])
    _plot_methods = dict(single='text', batched='text')

    selection_display = None

    def get_data(self, element, ranges, style):
        mapping = dict(x='x', y='y', text='text')
        if self.static_source:
            return dict(x=[], y=[], text=[]), mapping, style
        if self.invert_axes:
            data = dict(x=[element.y], y=[element.x])
        else:
            data = dict(x=[element.x], y=[element.y])
        self._categorize_data(data, ('x', 'y'), element.dimensions())
        data['text'] = [element.text]
        if 'text_align' not in style:
            style['text_align'] = element.halign
        baseline = 'middle' if element.valign == 'center' else element.valign
        if 'text_baseline' not in style:
            style['text_baseline'] = baseline
        if 'text_font_size' not in style:
            style['text_font_size'] = f"{int(element.fontsize)}Pt"
        if 'color' in style:
            style['text_color'] = style.pop('color')
        style['angle'] = np.deg2rad(style.get('angle', element.rotation))
        return (data, mapping, style)

    def get_batched_data(self, element, ranges=None):
        data = defaultdict(list)
        zorders = self._updated_zorders(element)
        for (_key, el), zorder in zip(element.data.items(), zorders, strict=None):
            style = self.lookup_options(element.last, 'style')
            style = style.max_cycles(len(self.ordering))[zorder]
            eldata, elmapping, style = self.get_data(el, ranges, style)
            for k, eld in eldata.items():
                data[k].extend(eld)
        return data, elmapping, style

    def get_extents(self, element, ranges=None, range_type='combined', **kwargs):
        return None, None, None, None




class LabelsPlot(ColorbarPlot, AnnotationPlot):

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    xoffset = param.Number(default=None, doc="""
      Amount of offset to apply to labels along x-axis.""")

    yoffset = param.Number(default=None, doc="""
      Amount of offset to apply to labels along x-axis.""")

    # Deprecated options

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
        Deprecated in favor of color style mapping, e.g. `color=dim('color')`""")

    selection_display = BokehOverlaySelectionDisplay()

    style_opts = [
        *base_properties,
        *text_properties,
        *background_properties,
        *border_properties,
        'cmap',
        'angle',
        'text_outline_color',
        *(("text_outline_width",) if BOKEH_GE_3_9_0 else ())
    ]

    _nonvectorized_styles = [*base_properties, 'cmap']

    _plot_methods = dict(single='text', batched='text')
    _batched_style_opts = text_properties + background_properties + border_properties

    def get_data(self, element, ranges, style):
        style = self.style[self.cyclic_index]
        if 'angle' in style and isinstance(style['angle'], (int, float)):
            style['angle'] = np.deg2rad(style.get('angle', 0))

        dims = element.dimensions()
        coords = (1, 0) if self.invert_axes else (0, 1)
        xdim, ydim, tdim = (dimension_sanitizer(dims[i].name) for i in (*coords, 2))
        mapping = dict(x=xdim, y=ydim, text=tdim)
        data = {d: element.dimension_values(d) for d in (xdim, ydim)}
        if self.xoffset is not None:
            mapping['x'] = dodge(xdim, self.xoffset)
        if self.yoffset is not None:
            mapping['y'] = dodge(ydim, self.yoffset)
        data[tdim] = [dims[2].pprint_value(v) for v in element.dimension_values(2)]
        self._categorize_data(data, (xdim, ydim), element.dimensions())

        cdim = element.get_dimension(self.color_index)
        if cdim is None:
            return data, mapping, style

        cdata, cmapping = self._get_color_data(element, ranges, style, name='text_color')
        if dims[2] is cdim and cdata:
            # If color dim is same as text dim, rename color column
            data['text_color'] = cdata[tdim]
            mapping['text_color'] = dict(cmapping['text_color'], field='text_color')
        else:
            data.update(cdata)
            mapping.update(cmapping)
        return data, mapping, style



class LineAnnotationPlot(ElementPlot, AnnotationPlot):

    style_opts = [*line_properties, 'level', 'visible']

    apply_ranges = param.Boolean(default=False, doc="""
        Whether to include the annotation in axis range calculations.""")

    _allow_implicit_categories = False
    _plot_methods = dict(single='Span')

    selection_display = None

    def get_data(self, element, ranges, style):
        data, mapping = {}, {}
        dim = 'width' if isinstance(element, HLine) else 'height'
        if self.invert_axes:
            dim = 'width' if dim == 'height' else 'height'
        mapping['dimension'] = dim
        loc = element.data
        if isinstance(loc, datetime_types):
            loc = date_to_integer(loc)
        mapping['location'] = loc
        return (data, mapping, style)

    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        box = Span(level=properties.get('level', 'glyph'), **mapping)
        plot.renderers.append(box)
        return None, box

    def get_extents(self, element, ranges=None, range_type='combined', **kwargs):
        loc = element.data
        if isinstance(element, VLine):
            dim = 'y' if self.invert_axes else 'x'
        elif isinstance(element, HLine):
            dim = 'x' if self.invert_axes else 'y'
        ranges[dim]['soft'] = loc, loc
        return super().get_extents(element, ranges, range_type)


class BoxAnnotationPlot(ElementPlot, AnnotationPlot):

    apply_ranges = param.Boolean(default=False, doc="""
        Whether to include the annotation in axis range calculations.""")

    style_opts = line_properties + fill_properties + ['level', 'visible']

    _allow_implicit_categories = False
    _plot_methods = dict(single='BoxAnnotation')

    selection_display = None

    def get_data(self, element, ranges, style):
        data = {}
        mapping = {k: None for k in ('left', 'right', 'bottom', 'top')}
        kwd_dim1 = 'left' if isinstance(element, VSpan) else 'bottom'
        kwd_dim2 = 'right' if isinstance(element, VSpan) else 'top'
        if self.invert_axes:
            kwd_dim1 = 'bottom' if kwd_dim1 == 'left' else 'left'
            kwd_dim2 = 'top' if kwd_dim2 == 'right' else 'right'

        locs = element.data
        if isinstance(locs, datetime_types):
            locs = [date_to_integer(loc) for loc in locs]
        mapping[kwd_dim1] = locs[0]
        mapping[kwd_dim2] = locs[1]
        return (data, mapping, style)

    def _update_glyph(self, renderer, properties, mapping, glyph, source, data):
        glyph.visible = any(v is not None for v in mapping.values())
        return super()._update_glyph(renderer, properties, mapping, glyph, source, data)

    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        box = BoxAnnotation(level=properties.get('level', 'glyph'), **mapping)
        plot.renderers.append(box)
        return None, box


class SlopePlot(ElementPlot, AnnotationPlot):

    style_opts = [*line_properties, 'level']

    _plot_methods = dict(single='Slope')

    selection_display = None

    def get_data(self, element, ranges, style):
        data, mapping = {}, {}
        gradient, intercept = element.data
        if self.invert_axes:
            if gradient == 0:
                gradient = np.inf, np.inf
            else:
                gradient, intercept = 1/gradient, -(intercept/gradient)
        mapping['gradient'] = gradient
        mapping['y_intercept'] = intercept
        return (data, mapping, style)

    def _init_glyph(self, plot, mapping, properties):
        """Returns a Bokeh glyph object.

        """
        slope = Slope(level=properties.get('level', 'glyph'), **mapping)
        plot.add_layout(slope)
        return None, slope

    def get_extents(self, element, ranges=None, range_type='combined', **kwargs):
        return None, None, None, None



class SplinePlot(ElementPlot, AnnotationPlot):
    """Draw the supplied Spline annotation (see Spline docstring).
    Does not support matplotlib Path codes.

    """

    style_opts = [*line_properties, 'visible']
    _plot_methods = dict(single='bezier')

    selection_display = None

    def get_data(self, element, ranges, style):
        if self.invert_axes:
            data_attrs = ['y0', 'x0', 'cy0', 'cx0', 'cy1', 'cx1', 'y1', 'x1']
        else:
            data_attrs = ['x0', 'y0', 'cx0', 'cy0', 'cx1', 'cy1', 'x1', 'y1']
        verts = np.array(element.data[0])
        inds = np.where(np.array(element.data[1])==1)[0]
        data = {da: [] for da in data_attrs}
        skipped = False
        for vs in np.split(verts, inds[1:]):
            if len(vs) != 4:
                skipped = len(vs) > 1
                continue
            for x, y, xl, yl in zip(vs[:, 0], vs[:, 1], data_attrs[::2], data_attrs[1::2], strict=None):
                data[xl].append(x)
                data[yl].append(y)
        if skipped:
            self.param.warning(
                'Bokeh SplinePlot only support cubic splines, unsupported '
                'splines were skipped during plotting.')
        data = {da: data[da] for da in data_attrs}
        return (data, dict(zip(data_attrs, data_attrs, strict=None)), style)



class ArrowPlot(CompositeElementPlot, AnnotationPlot):

    style_opts = ([f'arrow_{p}' for p in line_properties+fill_properties+['size']] +
                  text_properties)

    _style_groups = {'arrow': 'arrow', 'text': 'text'}

    _draw_order = ['arrow_1', 'text_1']

    selection_display = None

    def get_data(self, element, ranges, style):
        plot = self.state
        label_mapping = dict(x='x', y='y', text='text')
        arrow_mapping = dict(x_start='x_start', x_end='x_end',
                             y_start='y_start', y_end='y_end')

        # Compute arrow
        x1, y1 = element.x, element.y
        axrange = plot.x_range if self.invert_axes else plot.y_range
        span = (axrange.end - axrange.start) / 6.
        if element.direction == '^':
            x2, y2 = x1, y1-span
            label_mapping['text_baseline'] = 'top'
        elif element.direction == '<':
            x2, y2 = x1+span, y1
            label_mapping['text_align'] = 'left'
            label_mapping['text_baseline'] = 'middle'
        elif element.direction == '>':
            x2, y2 = x1-span, y1
            label_mapping['text_align'] = 'right'
            label_mapping['text_baseline'] = 'middle'
        else:
            x2, y2 = x1, y1+span
            label_mapping['text_baseline'] = 'bottom'
        arrow_data = {'x_end': [x1], 'y_end': [y1],
                      'x_start': [x2], 'y_start': [y2]}

        # Define arrowhead
        arrow_mapping['arrow_start'] = arrow_start.get(element.arrowstyle, None)
        arrow_mapping['arrow_end'] = arrow_end.get(element.arrowstyle, NormalHead)

        # Compute label
        if self.invert_axes:
            label_data = dict(x=[y2], y=[x2])
        else:
            label_data = dict(x=[x2], y=[y2])
        label_data['text'] = [element.text]
        return ({'text_1': label_data, 'arrow_1': arrow_data},
                {'arrow_1': arrow_mapping, 'text_1': label_mapping}, style)


    def _init_glyph(self, plot, mapping, properties, key):
        """Returns a Bokeh glyph object.

        """
        properties = {k: v for k, v in properties.items() if 'legend' not in k}

        if key == 'arrow_1':
            source = properties.pop('source')
            arrow_end = mapping.pop('arrow_end')
            arrow_start = mapping.pop('arrow_start')
            for p in ('alpha', 'color'):
                v = properties.pop(p, None)
                for t in ('line', 'fill'):
                    if v is None:
                        continue
                    key = f'{t}_{p}'
                    if key not in properties:
                        properties[key] = v
            start = arrow_start(**properties) if arrow_start else None
            end = arrow_end(**properties) if arrow_end else None
            line_props = {p: v for p, v in properties.items() if p.startswith('line_')}
            renderer = Arrow(start=start, end=end, source=source,
                             **dict(line_props, **mapping))
            glyph = renderer
        else:
            properties = {p if p == 'source' else 'text_'+p: v
                          for p, v in properties.items()}
            renderer, glyph = super()._init_glyph(
                plot, mapping, properties, key)
        plot.renderers.append(renderer)
        return renderer, glyph

    def get_extents(self, element, ranges=None, range_type='combined', **kwargs):
        return None, None, None, None



class DivPlot(BokehPlot, GenericElementPlot, AnnotationPlot):

    height = param.Number(default=300)

    width = param.Number(default=300)

    sizing_mode = param.Selector(default=None, objects=[
        'fixed', 'stretch_width', 'stretch_height', 'stretch_both',
        'scale_width', 'scale_height', 'scale_both', None], doc="""

        How the component should size itself.

        * "fixed" :
          Component is not responsive. It will retain its original
          width and height regardless of any subsequent browser window
          resize events.
        * "stretch_width"
          Component will responsively resize to stretch to the
          available width, without maintaining any aspect ratio. The
          height of the component depends on the type of the component
          and may be fixed or fit to component's contents.
        * "stretch_height"
          Component will responsively resize to stretch to the
          available height, without maintaining any aspect ratio. The
          width of the component depends on the type of the component
          and may be fixed or fit to component's contents.
        * "stretch_both"
          Component is completely responsive, independently in width
          and height, and will occupy all the available horizontal and
          vertical space, even if this changes the aspect ratio of the
          component.
        * "scale_width"
          Component will responsively resize to stretch to the
          available width, while maintaining the original or provided
          aspect ratio.
        * "scale_height"
          Component will responsively resize to stretch to the
          available height, while maintaining the original or provided
          aspect ratio.
        * "scale_both"
          Component will responsively resize to both the available
          width and height, while maintaining the original or provided
          aspect ratio.
    """)

    hooks = param.HookList(default=[], doc="""
        Optional list of hooks called when finalizing a plot. The
        hook is passed the plot object and the displayed element, and
        other plotting handles can be accessed via plot.handles.""")

    _stream_data = False

    selection_display = None

    def __init__(self, element, plot=None, **params):
        super().__init__(element, **params)
        self.callbacks = []
        self.handles = {} if plot is None else self.handles['plot']
        self.static = len(self.hmap) == 1 and len(self.keys) == len(self.hmap)

    def get_data(self, element, ranges, style):
        return element.data, {}, style

    def initialize_plot(self, ranges=None, plot=None, plots=None, source=None):
        """Initializes a new plot object with the last available frame.

        """
        # Get element key and ranges for frame
        element = self.hmap.last
        key = self.keys[-1]
        self.current_frame = element
        self.current_key = key

        data, _, _ = self.get_data(element, ranges, {})
        div = HTML(text=escape(data), width=self.width, height=self.height,
                   sizing_mode=self.sizing_mode)
        self.handles['plot'] = div
        self._execute_hooks(element)
        self.drawn = True
        return div

    def update_frame(self, key, ranges=None, plot=None):
        """Updates an existing plot with data corresponding
        to the key.

        """
        element = self._get_frame(key)
        text, _, _ = self.get_data(element, ranges, {})
        self.state.update(text=text, sizing_mode=self.sizing_mode)
