import numpy as np
import param
from bokeh.models import CustomJSHover
from bokeh.models.glyphs import AnnularWedge
from bokeh.models.ranges import FactorRange

from ...core.data import GridInterface
from ...core.spaces import HoloMap
from ...core.util import (
    dimension_sanitizer,
    dtype_kind,
    find_contiguous_subarray,
    is_nan,
)
from .element import ColorbarPlot, CompositeElementPlot
from .selection import BokehOverlaySelectionDisplay
from .styles import base_properties, fill_properties, line_properties, text_properties
from .util import BOKEH_GE_3_3_0


class HeatMapPlot(ColorbarPlot):

    clipping_colors = param.Dict(default={'NaN': 'white'}, doc="""
        Dictionary to specify colors for clipped values.
        Allows setting color for NaN values and for values above and below
        the min and max value. The min, max, or NaN color may specify
        an RGB(A) color as a either (1) a color hex string of the form
        #FFFFFF or #FFFFFFFF, (2) a length-3 or length-4 tuple specifying
        values in the range 0-1, or (3) a named HTML color.""")

    padding = param.ClassSelector(default=0, class_=(int, float, tuple))

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    radial = param.Boolean(default=False, doc="""
        Whether the HeatMap should be radial""")

    xmarks = param.Parameter(default=None, doc="""
        Add separation lines to the heatmap for better readability. By
        default, does not show any separation lines. If parameter is of type
        integer, draws the given amount of separations lines spread across
        heatmap. If parameter is of type list containing integers, show
        separation lines at given indices. If parameter is of type tuple, draw
        separation lines at given categorical values. If parameter is of type
        function, draw separation lines where function returns True for passed
        heatmap category.""")

    ymarks = param.Parameter(default=None, doc="""
        Add separation lines to the heatmap for better readability. By
        default, does not show any separation lines. If parameter is of type
        integer, draws the given amount of separations lines spread across
        heatmap. If parameter is of type list containing integers, show
        separation lines at given indices. If parameter is of type tuple, draw
        separation lines at given categorical values. If parameter is of type
        function, draw separation lines where function returns True for passed
        heatmap category.""")

    optimize_gridded = param.Boolean(default=True, doc="""
        Whether to optimize the heatmap rendering when the data is
        gridded and contiguous. If enabled, a single image glyph will be
        used to render the heatmap instead of individual quad glyphs,
        significantly improving performance for large heatmaps.""")


    style_opts = ['cmap', 'dilate', *base_properties, *line_properties, *fill_properties]

    selection_display = BokehOverlaySelectionDisplay()

    def __init__(self, element, plot=None, **params):
        super().__init__(element, plot=plot, **params)
        self._is_contiguous_gridded = False

    @property
    def is_contiguous_gridded(self):
        return self.optimize_gridded and self._is_contiguous_gridded

    @property
    def _plot_methods(self):
        return dict(single='image') if self.is_contiguous_gridded else dict(single='rect')

    @classmethod
    def is_radial(cls, heatmap):
        heatmap = heatmap.last if isinstance(heatmap, HoloMap) else heatmap
        opts = cls.lookup_options(heatmap, 'plot').options
        return ((any(o in opts for o in ('start_angle', 'radius_inner', 'radius_outer'))
                 and not (opts.get('radial') == False)) or opts.get('radial', False))

    def _get_factors(self, element, ranges):
        return super()._get_factors(element.gridded, ranges)

    def _element_transform(self, transform, element, ranges):
        return transform.apply(element.gridded, ranges=ranges, flat=False).T.flatten()

    def _update_hover(self, element):
        hover = self.handles["hover"]
        if not self.is_contiguous_gridded or 'hv_created' not in hover.tags:
            return super()._update_hover(element)

        source = self.handles["cds"]
        x_range = self.handles["x_range"]
        y_range = self.handles["y_range"]

        pixel_image = CustomJSHover(
            args=dict(src=source, x_range=x_range, y_range=y_range),
            code="""
            const formatter = Bokeh.require("core/util/templating").get_formatter();
            const data = src.data;
            const [ny, nx] = data.image[0].shape;
            const {x, y} = special_vars;
            if (format == "x") {
              const x0 = data.x[0];
              const dx = data.dw[0] / nx;
              const ix = Math.floor((x - x0) / dx);
              const factors = x_range.factors ? x_range.factors[ix] : formatter(x0 + dx / 2 + ix * dx)
              return (ix >= 0 && ix < nx) ? factors : "-";
            } else {
              const y0 = data.y[0];
              const dy = data.dh[0] / ny;
              const iy = Math.floor((y - y0) / dy);
              const factors = y_range.factors ? y_range.factors[iy] : formatter(y0 + dy / 2 + iy * dy)
              return (iy >= 0 && iy < ny) ? factors : "-";
            }""",
        )

        if BOKEH_GE_3_3_0:
            xdim, ydim = element.kdims
            vdim = ", ".join([d.pprint_label for d in element.vdims])
            hover.tooltips = [(xdim.pprint_label, "$x{x}"), (ydim.pprint_label, "$y{y}"), (vdim, '@image')]
        else:
            xdim, ydim = element.kdims
            hover.tooltips = [(xdim.pprint_label, "$x{x}"), (ydim.pprint_label, "$y{y}")]
        hover.formatters = {"$x": pixel_image, "$y": pixel_image}

    def get_data(self, element, ranges, style):
        x, y = (dimension_sanitizer(d) for d in element.dimensions(label=True)[:2])
        if self.invert_axes:
            x, y = y, x
        cmapper = self._get_colormapper(element.vdims[0], element, ranges, style)

        if not element._unique:
            self.param.warning('HeatMap element index is not unique,  ensure you '
                               'aggregate the data before displaying it, e.g. '
                               'using heatmap.aggregate(function=np.mean). '
                               'Duplicate index values have been dropped.')

        is_gridded = element.interface.gridded
        x_index = y_index = None
        if is_gridded and self.optimize_gridded:
            x_range, y_range = self.handles['x_range'], self.handles['y_range']
            x_cat, y_cat = isinstance(x_range, FactorRange), isinstance(y_range, FactorRange)
            if x_cat:
                xs = self._get_dimension_factors(element, ranges, element.get_dimension(x))
                x_index = find_contiguous_subarray(xs, x_range.factors) if x_range.factors else 0
            else:
                xs = element.dimension_values(x, expanded=False)
                xdiff = np.diff(xs)
                x_index = ranges[x]['data'][0] if (not xdiff.size or np.allclose(xdiff, xdiff[0])) else None
            if y_cat:
                ys = self._get_dimension_factors(element, ranges, element.get_dimension(y))
                y_index = find_contiguous_subarray(ys, y_range.factors) if y_range.factors else 0
            else:
                ys = element.dimension_values(y, expanded=False)
                ydiff = np.diff(ys)
                y_index = ranges[y]['data'][0] if (not ydiff.size or np.allclose(ydiff, ydiff[0])) else None

        self._is_contiguous_gridded = is_gridded and x_index is not None and y_index is not None
        if self.is_contiguous_gridded:
            style = {
                k: v for k, v in style.items()
                if not (
                    k.startswith(('annular_', 'xmarks_', 'ymarks_',)) or "fill" in k or "line" in k or "color" in k
                )
            }
            style['color_mapper'] = cmapper
            mapping = dict(image='image', x='x', y='y', dw='dw', dh='dh')
            if self.static_source:
                return {}, mapping, style
            if 'alpha' in style:
                style['global_alpha'] = style['alpha']
            data = dict(x=[x_index], y=[y_index])
            for i, vdim in enumerate(element.vdims, 2):
                if i > 2 and 'hover' not in self.handles:
                    break
                img = element.dimension_values(i, flat=False)
                if dtype_kind(img) == 'b':
                    img = img.astype(np.int8)
                if 0 in img.shape:
                    img = np.array([[np.nan]])
                if self.invert_axes:
                    img = img.T
                key = 'image' if i == 2 else dimension_sanitizer(vdim.name)
                data[key] = [img]
            dw = data['image'][0].shape[1] if x_cat else (ranges[x]['data'][1] - ranges[x]['data'][0])
            dh = data['image'][0].shape[0] if y_cat else (ranges[y]['data'][1] - ranges[y]['data'][0])
            data['dh'], data['dw'] = [dh], [dw]
            return data, mapping, style
        elif self.static_source:
            return {}, {'x': x, 'y': y, 'fill_color': {'field': 'zvalues', 'transform': cmapper}}, style

        if 'line_alpha' not in style and 'line_width' not in style:
            style['line_alpha'] = 0
            style['selection_line_alpha'] = 0
            style['nonselection_line_alpha'] = 0
        elif 'line_color' not in style:
            style['line_color'] = 'white'

        aggregate = element.gridded
        xtype = aggregate.interface.dtype(aggregate, x)
        widths = None
        if xtype.kind in 'SUO':
            xvals = aggregate.dimension_values(x)
            width = 1
        else:
            xvals = aggregate.dimension_values(x, flat=False)
            if self.invert_axes:
                xvals = xvals.T
            if xvals.shape[1] > 1:
                edges = GridInterface._infer_interval_breaks(xvals, axis=1)
                widths = np.diff(edges, axis=1).T.flatten()
            else:
                widths = [self.default_span]*xvals.shape[0] if len(xvals) else []
            xvals = xvals.T.flatten()
            width = 'width'

        ytype = aggregate.interface.dtype(aggregate, y)
        heights = None
        if ytype.kind in 'SUO':
            yvals = aggregate.dimension_values(y)
            height = 1
        else:
            yvals = aggregate.dimension_values(y, flat=False)
            if self.invert_axes:
                yvals = yvals.T
            if yvals.shape[0] > 1:
                edges = GridInterface._infer_interval_breaks(yvals, axis=0)
                heights = np.diff(edges, axis=0).T.flatten()
            else:
                heights = [self.default_span]*yvals.shape[1] if len(yvals) else []
            yvals = yvals.T.flatten()
            height = 'height'

        zvals = aggregate.dimension_values(2, flat=False)
        if not self.invert_axes:
            zvals = zvals.T
        zvals = zvals.flatten()

        data = {x: xvals, y: yvals, 'zvalues': zvals}
        if widths is not None:
            data['width'] = widths
        if heights is not None:
            data['height'] = heights

        if 'hover' in self.handles and not self.static_source:
            for vdim in element.vdims:
                sanitized = dimension_sanitizer(vdim.name)
                data[sanitized] = ['-' if is_nan(v) else vdim.pprint_value(v)
                                   for v in aggregate.dimension_values(vdim)]

        # Filter radial heatmap options
        style = {k: v for k, v in style.items() if not
                 any(g in k for g in RadialHeatMapPlot._style_groups.values())}
        return (data, {'x': x, 'y': y, 'fill_color': {'field': 'zvalues', 'transform': cmapper},
                       'height': height, 'width': width}, style)

    def _draw_markers(self, plot, element, marks, axis='x'):
        if marks is None or self.radial:
            return
        self.param.warning('Only radial HeatMaps supports marks, to make the'
                           'HeatMap quads for distinguishable set a line_width')

    def _init_glyphs(self, plot, element, ranges, source):
        super()._init_glyphs(plot, element, ranges, source)
        self._draw_markers(plot, element, self.xmarks, axis='x')
        self._draw_markers(plot, element, self.ymarks, axis='y')
        if "hover" in self.handles:
            self._update_hover(element)

    def _update_glyphs(self, element, ranges, style):
        super()._update_glyphs(element, ranges, style)
        plot = self.handles['plot']
        self._draw_markers(plot, element, self.xmarks, axis='x')
        self._draw_markers(plot, element, self.ymarks, axis='y')


class RadialHeatMapPlot(CompositeElementPlot, ColorbarPlot):

    clipping_colors = param.Dict(default={'NaN': 'white'}, doc="""
        Dictionary to specify colors for clipped values.
        Allows setting color for NaN values and for values above and below
        the min and max value. The min, max, or NaN color may specify
        an RGB(A) color as a either (1) a color hex string of the form
        #FFFFFF or #FFFFFFFF, (2) a length-3 or length-4 tuple specifying
        values in the range 0-1, or (3) a named HTML color.""")

    start_angle = param.Number(default=np.pi/2, doc="""
        Define starting angle of the first annulus segment. By default, begins
        at 12 o'clock.""")

    radius_inner = param.Number(default=0.1, bounds=(0, 0.5), doc="""
        Define the radius fraction of inner, empty space.""")

    radius_outer = param.Number(default=0.05, bounds=(0, 1), doc="""
        Define the radius fraction of outer space including the labels.""")

    xmarks = param.Parameter(default=None, doc="""
        Add separation lines between segments for better readability. By
        default, does not show any separation lines. If parameter is of type
        integer, draws the given amount of separations lines spread across
        radial heatmap. If parameter is of type list containing integers, show
        separation lines at given indices. If parameter is of type tuple, draw
        separation lines at given segment values. If parameter is of type
        function, draw separation lines where function returns True for passed
        segment value.""")

    ymarks = param.Parameter(default=None, doc="""
        Add separation lines between annulars for better readability. By
        default, does not show any separation lines. If parameter is of type
        integer, draws the given amount of separations lines spread across
        radial heatmap. If parameter is of type list containing integers, show
        separation lines at given indices. If parameter is of type tuple, draw
        separation lines at given annular values. If parameter is of type
        function, draw separation lines where function returns True for passed
        annular value.""")

    max_radius = param.Number(default=0.5, doc="""
        Define the maximum radius which is used for the x and y range extents.
        """)

    radial = param.Boolean(default=True, doc="""
        Whether the HeatMap should be radial""")

    show_frame = param.Boolean(default=False, doc="""
        Whether or not to show a complete frame around the plot.""")

    xticks = param.Parameter(default=4, doc="""
        Ticks along x-axis/segments specified as an integer, explicit list of
        ticks or function. If `None`, no ticks are shown.""")

    yticks = param.Parameter(default=4, doc="""
        Ticks along y-axis/annulars specified as an integer, explicit list of
        ticks or function. If `None`, no ticks are shown.""")

    yrotation = param.Number(default=90, doc="""
        Define angle along which yticks/annulars are shown. By default, yticks
        are drawn like a regular y-axis.""")

    # Map each glyph to a style group
    _style_groups = {'annular_wedge': 'annular',
                     'text': 'ticks',
                     'multi_line': 'xmarks',
                     'arc': 'ymarks'}

    _draw_order = ['annular_wedge', 'multi_line', 'arc', 'text']

    style_opts = (['xmarks_' + p for p in base_properties + line_properties] + \
                  ['ymarks_' + p for p in base_properties + line_properties] + \
                  ['annular_' + p for p in base_properties + fill_properties + line_properties] + \
                  ['ticks_' + p for p in text_properties] + ['cmap'])

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.xaxis = None
        self.yaxis = None

    def _get_bins(self, kind, order, reverse=False):
        """Map elements from given `order` array to bins of start and end values
        for radius or angle dimension.

        """
        if kind == "radius":
            start = self.max_radius * self.radius_inner
            end = self.max_radius

        elif kind == "angle":
            start = self.start_angle
            end = self.start_angle + 2 * np.pi

        bounds = np.linspace(start, end, len(order) + 1)
        bins = np.vstack([bounds[:-1], bounds[1:]]).T

        if reverse:
            bins = bins[::-1]

        return dict(zip(order, bins, strict=None))

    @staticmethod
    def _get_bounds(mapper, values):
        """Extract first and second value from tuples of mapped bins.

        """
        array = np.array([mapper.get(x) for x in values])
        return array[:, 0], array[:, 1]

    def _postprocess_hover(self, renderer, source):
        """Limit hover tool to annular wedges only.

        """
        if isinstance(renderer.glyph, AnnularWedge):
            super()._postprocess_hover(renderer, source)

    def get_extents(self, view, ranges, range_type='combined', **kwargs):
        """Supply custom, static extents because radial heatmaps always have
        the same boundaries.

        """
        if range_type not in ('data', 'combined'):
            return (None,)*4
        lower = -self.radius_outer
        upper = 2 * self.max_radius + self.radius_outer
        return (lower, lower, upper, upper)

    def _get_axis_dims(self, element):
        return (None, None)

    def _axis_properties(self, *args, **kwargs):
        """Overwrite default axis properties handling due to clashing
        categorical input and numerical output axes.

        Axis properties are handled separately for radial heatmaps because of
        missing radial axes in bokeh.

        """
        return {}

    def get_default_mapping(self, z, cmapper):
        """Create dictionary containing default ColumnDataSource glyph to data
        mappings.

        """
        map_annular = dict(x=self.max_radius, y=self.max_radius,
                           inner_radius="inner_radius",
                           outer_radius="outer_radius",
                           start_angle="start_angle",
                           end_angle="end_angle",
                           fill_color={'field': z, 'transform': cmapper})

        map_seg_label = dict(x="x", y="y", text="text",
                             angle="angle", text_align="center")

        map_ann_label = dict(x="x", y="y", text="text",
                             angle="angle", text_align="center",
                             text_baseline="bottom")

        map_xmarks = dict(xs="xs", ys="ys")

        map_ymarks = dict(x= self.max_radius, y=self.max_radius,
                          start_angle=0, end_angle=2*np.pi, radius="radius")

        return {'annular_wedge_1': map_annular,
                'text_1': map_seg_label,
                'text_2': map_ann_label,
                'multi_line_1': map_xmarks,
                'arc_1': map_ymarks}

    def _pprint(self, element, dim_label, vals):
        """Helper function to convert values to corresponding dimension type.

        """
        if dtype_kind(vals) not in 'SU':
            dim = element.gridded.get_dimension(dim_label)
            return [dim.pprint_value(v) for v in vals]

        return vals

    def _compute_tick_mapping(self, kind, order, bins):
        """Helper function to compute tick mappings based on `ticks` and
        default orders and bins.

        """
        if kind == "angle":
            ticks = self.xticks
            reverse = True
        elif kind == "radius":
            ticks = self.yticks
            reverse = False

        if callable(ticks):
            text_nth = [x for x in order if ticks(x)]

        elif isinstance(ticks, (tuple, list)):
            bins = self._get_bins(kind, ticks, reverse)
            text_nth = ticks

        elif ticks:
            nth_label = np.ceil(len(order) / float(ticks)).astype(int)
            text_nth = order[::nth_label]

        return {x: bins[x] for x in text_nth}

    def _get_seg_labels_data(self, order_seg, bins_seg):
        """Generate ColumnDataSource dictionary for segment labels.

        """
        if self.xticks is None:
            return dict(x=[], y=[], text=[], angle=[])

        mapping = self._compute_tick_mapping("angle", order_seg, bins_seg)

        values = [(text, ((end - start) / 2) + start)
                  for text, (start, end) in mapping.items()]

        labels, radiant = zip(*values, strict=None)
        radiant = np.array(radiant)

        y_coord = np.sin(radiant) * self.max_radius + self.max_radius
        x_coord = np.cos(radiant) * self.max_radius + self.max_radius

        return dict(x=x_coord,
                    y=y_coord,
                    text=labels,
                    angle=1.5 * np.pi + radiant)

    def _get_ann_labels_data(self, order_ann, bins_ann):
        """Generate ColumnDataSource dictionary for annular labels.

        """
        if self.yticks is None:
            return dict(x=[], y=[], text=[], angle=[])

        mapping = self._compute_tick_mapping("radius", order_ann, bins_ann)
        values = [(label, radius[0]) for label, radius in mapping.items()]

        labels, radius = zip(*values, strict=None)
        radius = np.array(radius)

        y_coord = np.sin(np.deg2rad(self.yrotation)) * radius + self.max_radius
        x_coord = np.cos(np.deg2rad(self.yrotation)) * radius + self.max_radius

        return dict(x=x_coord,
                    y=y_coord,
                    text=labels,
                    angle=[0]*len(labels))

    @staticmethod
    def _get_markers(marks, order, bins):
        """Helper function to get marker positions depending on mark type.

        """
        if callable(marks):
            markers = [x for x in order if marks(x)]
        elif isinstance(marks, list):
            markers = [order[x] for x in marks]
        elif isinstance(marks, tuple):
            markers = marks
        else:
            nth_mark = np.ceil(len(order) / marks).astype(int)
            markers = order[::nth_mark]

        return np.array([bins[x][1] for x in markers])

    def _get_xmarks_data(self, order_seg, bins_seg):
        """Generate ColumnDataSource dictionary for segment separation lines.

        """
        if not self.xmarks:
            return dict(xs=[], ys=[])

        angles = self._get_markers(self.xmarks, order_seg, bins_seg)

        inner = self.max_radius * self.radius_inner
        outer = self.max_radius

        y_start = np.sin(angles) * inner + self.max_radius
        y_end = np.sin(angles) * outer + self.max_radius

        x_start = np.cos(angles) * inner + self.max_radius
        x_end = np.cos(angles) * outer + self.max_radius

        xs = zip(x_start, x_end, strict=None)
        ys = zip(y_start, y_end, strict=None)

        return dict(xs=list(xs), ys=list(ys))

    def _get_ymarks_data(self, order_ann, bins_ann):
        """Generate ColumnDataSource dictionary for segment separation lines.

        """
        if not self.ymarks:
            return dict(radius=[])

        radius = self._get_markers(self.ymarks, order_ann, bins_ann)

        return dict(radius=radius)

    def get_data(self, element, ranges, style):
        # dimension labels
        dim_labels = element.dimensions(label=True)[:3]
        x, y, z = (dimension_sanitizer(d) for d in dim_labels)
        if self.invert_axes: x, y = y, x

        # color mapper
        cmapper = self._get_colormapper(element.vdims[0], element,
                                        ranges, style)

        # default CDS data mapping
        mapping = self.get_default_mapping(z, cmapper)
        if self.static_source:
            return {}, mapping, style

        # get raw values
        aggregate = element.gridded
        xvals = aggregate.dimension_values(x)
        yvals = aggregate.dimension_values(y)
        zvals = aggregate.dimension_values(2, flat=True)

        # get orders
        order_seg = aggregate.dimension_values(x, expanded=False)
        order_ann = aggregate.dimension_values(y, expanded=False)

        # pretty print if necessary
        xvals = self._pprint(element, x, xvals)
        yvals = self._pprint(element, y, yvals)
        order_seg = self._pprint(element, x, order_seg)
        order_ann = self._pprint(element, y, order_ann)

        # annular wedges
        bins_ann = self._get_bins("radius", order_ann)
        if len(bins_ann):
            inner_radius, outer_radius = self._get_bounds(bins_ann, yvals)
            data_text_ann = self._get_ann_labels_data(order_ann, bins_ann)
        else:
            inner_radius, outer_radius =  [], []
            data_text_ann = dict(x=[], y=[], text=[], angle=[])

        bins_seg = self._get_bins("angle", order_seg, True)
        if len(bins_seg):
            start_angle, end_angle = self._get_bounds(bins_seg, xvals)
            data_text_seg = self._get_seg_labels_data(order_seg, bins_seg)
        else:
            start_angle, end_angle = [], []
            data_text_seg = dict(x=[], y=[], text=[], angle=[])

        # create ColumnDataSources
        data_annular = {"start_angle": start_angle,
                        "end_angle": end_angle,
                        "inner_radius": inner_radius,
                        "outer_radius": outer_radius,
                        z: zvals, x: xvals, y: yvals}

        if 'hover' in self.handles:
            for vdim in element.vdims:
                sanitized = dimension_sanitizer(vdim.name)
                values = ['-' if is_nan(v) else vdim.pprint_value(v)
                          for v in aggregate.dimension_values(vdim)]
                data_annular[sanitized] = values

        data_xmarks = self._get_xmarks_data(order_seg, bins_seg)
        data_ymarks = self._get_ymarks_data(order_ann, bins_ann)

        data = {'annular_wedge_1': data_annular,
                'text_1': data_text_seg,
                'text_2': data_text_ann,
                'multi_line_1': data_xmarks,
                'arc_1': data_ymarks}

        return data, mapping, style

    def _init_glyph(self, plot, mapping, properties, key):
        ret = super()._init_glyph(plot, mapping, properties, key)
        if self.colorbar and 'color_mapper' in self.handles:
            self._draw_colorbar(plot, self.handles['color_mapper'])
        return ret
