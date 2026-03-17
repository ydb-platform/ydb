from collections import defaultdict
from itertools import groupby

import numpy as np
import param
from bokeh.layouts import gridplot
from bokeh.models import (
    Axis,
    ColorBar,
    Column,
    ColumnDataSource,
    Div,
    Legend,
    Row,
    Title,
)
from bokeh.models.layouts import TabPanel, Tabs

from ...core import (
    AdjointLayout,
    Element,
    Empty,
    GridSpace,
    HoloMap,
    Layout,
    NdLayout,
    Store,
)
from ...core.options import SkipRendering
from ...core.util import (
    _STANDARD_CALENDARS,
    cftime_to_timestamp,
    cftime_types,
    get_method_owner,
    is_param_method,
    unique_iterator,
    wrap_tuple,
    wrap_tuple_streams,
)
from ...selection import NoOpSelectionDisplay
from ..links import Link
from ..plot import (
    CallbackPlot,
    DimensionedPlot,
    GenericAdjointLayoutPlot,
    GenericCompositePlot,
    GenericElementPlot,
    GenericLayoutPlot,
    GenericOverlayPlot,
)
from ..util import attach_streams, collate, displayable
from .links import LinkCallback
from .util import (
    cds_column_replace,
    decode_bytes,
    empty_plot,
    filter_toolboxes,
    get_default,
    make_axis,
    merge_tools,
    select_legends,
    sync_legends,
    theme_attr_json,
    update_shared_sources,
)


class BokehPlot(DimensionedPlot, CallbackPlot):
    """Plotting baseclass for the Bokeh backends, implementing the basic
    plotting interface for Bokeh based plots.

    """

    shared_datasource = param.Boolean(default=True, doc="""
        Whether Elements drawing the data from the same object should
        share their Bokeh data source allowing for linked brushing
        and other linked behaviors.""")

    title = param.String(default="{label} {group} {dimensions}", doc="""
        The formatting string for the title of this plot, allows defining
        a label group separator and dimension labels.""")

    title_format = param.String(default=None, doc="Alias for title.")

    toolbar = param.Selector(default='above',
                                   objects=["above", "below",
                                            "left", "right", None],
                                   doc="""
        The toolbar location, must be one of 'above', 'below',
        'left', 'right', None.""",
    )

    autohide_toolbar = param.Boolean(
        default=False,
        doc="""
        Whether to automatically hide the toolbar until the user hovers over the plot.
        This parameter has no effect if the toolbar is disabled (toolbar=None).""",
    )

    width = param.Integer(default=None, bounds=(0, None), doc="""
        The width of the component (in pixels). This can be either
        fixed or preferred width, depending on width sizing policy.""")

    height = param.Integer(default=None, bounds=(0, None), doc="""
        The height of the component (in pixels).  This can be either
        fixed or preferred height, depending on height sizing policy.""")

    _merged_tools = ['pan', 'box_zoom', 'box_select', 'lasso_select',
                     'poly_select', 'ypan', 'xpan']

    _title_template = (
        '<span style='
        '"color:{color};font-family:{font};'
        'font-style:{fontstyle};font-weight:{fontstyle};'  # italic/bold
        'font-size:{fontsize}">'
        '{title}</span>'
    )

    backend = 'bokeh'

    selection_display = NoOpSelectionDisplay()

    @property
    def id(self):
        return self.root.ref['id'] if self.root else None


    def get_data(self, element, ranges, style):
        """Returns the data from an element in the appropriate format for
        initializing or updating a ColumnDataSource and a dictionary
        which maps the expected keywords arguments of a glyph to
        the column in the datasource.

        """
        raise NotImplementedError


    def _update_selected(self, cds):
        from .callbacks import Selection1DCallback
        cds.selected.indices = self.selected
        for cb in self.callbacks:
            if isinstance(cb, Selection1DCallback):
                for s in cb.streams:
                    s.update(index=self.selected)

    def _init_datasource(self, data):
        """Initializes a data source to be passed into the bokeh glyph.

        """
        data = self._postprocess_data(data)
        cds = ColumnDataSource(data=data)
        if hasattr(self, 'selected')  and self.selected is not None:
            self._update_selected(cds)
        return cds


    def _postprocess_data(self, data):
        """Applies necessary type transformation to the data before
        it is set on a ColumnDataSource.

        """
        new_data = {}
        for k, values in data.items():
            values = decode_bytes(values) # Bytes need decoding to strings
            # Certain datetime types need to be converted
            if len(values) and isinstance(values[0], cftime_types):
                if any(v.calendar not in _STANDARD_CALENDARS for v in values):
                    self.param.warning(
                        'Converting cftime.datetime from a non-standard '
                        f'calendar ({values[0].calendar}) to a standard calendar for plotting. '
                        'This may lead to subtle errors in formatting '
                        'dates, for accurate tick formatting switch to '
                        'the matplotlib backend.')
                values = cftime_to_timestamp(values, 'ms')
            new_data[k] = values
        return new_data


    def _update_datasource(self, source, data):
        """Update datasource with data for a new frame.

        """
        data = self._postprocess_data(data)
        empty = all(len(v) == 0 for v in data.values())
        if (self.streaming and self.streaming[0].data is self.current_frame.data
            and self._stream_data and not empty):
            stream = self.streaming[0]
            if stream._count == ((self._stream_count or 0) + 1):
                if stream._triggering and stream.following:
                    data = {k: v[-stream._chunk_length:] for k, v in data.items()}
                    source.stream(data, stream.length)
                    self._stream_count = stream._count
                return
            elif not stream.following:
                return
            self._stream_count = stream._count

        if cds_column_replace(source, data):
            source.data = data
        else:
            source.data.update(data)

        if hasattr(self, 'selected') and self.selected is not None:
            self._update_selected(source)


    @property
    def state(self):
        """The plotting state that gets updated via the update method and
        used by the renderer to generate output.

        """
        return self.handles['plot']


    @property
    def current_handles(self):
        """Should return a list of plot objects that have changed and
        should be updated.

        """
        return []


    def _get_fontsize_defaults(self):
        theme = self.renderer.theme
        defaults = {
            'title': get_default(Title, 'text_font_size', theme),
            'legend_title': get_default(Legend, 'title_text_font_size', theme),
            'legend': get_default(Legend, 'label_text_font_size', theme),
            'label': get_default(Axis, 'axis_label_text_font_size', theme),
            'ticks': get_default(Axis, 'major_label_text_font_size', theme),
            'cticks': get_default(ColorBar, 'major_label_text_font_size', theme),
            'clabel': get_default(ColorBar, 'title_text_font_size', theme)
        }
        processed = dict(defaults)
        for k, v in defaults.items():
            if isinstance(v, dict) and 'value' in v:
                processed[k] = v['value']
        return processed


    def cleanup(self):
        """Cleans up references to the plot after the plot has been
        deleted. Traverses through all plots cleaning up Callbacks and
        Stream subscribers.

        """
        plots = self.traverse(lambda x: x, [BokehPlot])
        for plot in plots:
            if not isinstance(plot, (GenericCompositePlot, GenericElementPlot, GenericOverlayPlot)):
                continue
            streams = list(plot.streams)
            plot.streams = []
            plot._document = None

            if plot.subplots:
                plot.subplots.clear()

            if not isinstance(plot, (GenericElementPlot, GenericOverlayPlot)):
                continue

            for callback in plot.callbacks:
                streams += callback.streams
                callback.cleanup()

            for stream in set(streams):
                stream._subscribers = [
                    (p, subscriber) for p, subscriber in stream._subscribers
                    if not is_param_method(subscriber) or
                    get_method_owner(subscriber) not in plots
                ]

    def _fontsize(self, key, label='fontsize', common=True):
        """Converts integer fontsizes to a string specifying
        fontsize in pt.

        """
        size = super()._fontsize(key, label, common)
        return {k: v if isinstance(v, str) else f'{v}pt'
                for k, v in size.items()}

    def _get_title_div(self, key, default_fontsize='15pt', width=450):
        title_div = None
        title = self._format_title(key) if self.show_title else ''
        if not title:
            return title_div

        title_json = theme_attr_json(self.renderer.theme, 'Title')
        color = title_json.get('text_color', None)
        font = title_json.get('text_font', 'Arial')
        fontstyle = title_json.get('text_font_style', 'bold')
        fontsize = self._fontsize('title').get('fontsize', default_fontsize)
        if fontsize == default_fontsize:  # if default
            fontsize = title_json.get('text_font_size', default_fontsize)
            if 'em' in fontsize:
                # it's smaller than it shosuld be so add 0.25
                fontsize = str(float(fontsize[:-2]) + 0.25) + 'em'

        title_tags = self._title_template.format(
            color=color,
            font=font,
            fontstyle=fontstyle,
            fontsize=fontsize,
            title=title)

        if 'title' in self.handles:
            title_div = self.handles['title']
        else:
            # so it won't wrap long titles easily
            title_div = Div(width=width, styles={"white-space": "nowrap"})
        title_div.text = title_tags

        return title_div

    def sync_sources(self):
        """Syncs data sources between Elements, which draw data
        from the same object.

        """
        get_sources = lambda x: (id(x.current_frame.data), x)
        filter_fn = lambda x: (x.shared_datasource and x.current_frame is not None and
                               not isinstance(x.current_frame.data, np.ndarray)
                               and 'source' in x.handles)
        data_sources = self.traverse(get_sources, [filter_fn])
        grouped_sources = groupby(sorted(data_sources, key=lambda x: x[0]), lambda x: x[0])
        shared_sources = []
        source_cols = {}
        plots = []
        for _, group in grouped_sources:
            group = list(group)
            if len(group) > 1:
                source_data = {}
                for _, plot in group:
                    source_data.update(plot.handles['source'].data)
                new_source = ColumnDataSource(source_data)
                for _, plot in group:
                    renderer = plot.handles.get('glyph_renderer')
                    for callback in plot.callbacks:
                        callback.reset()
                    if renderer is None:
                        continue
                    elif 'data_source' in renderer.properties():
                        renderer.update(data_source=new_source)
                    else:
                        renderer.update(source=new_source)
                    plot.handles['source'] = plot.handles['cds'] = new_source
                    plots.append(plot)
                shared_sources.append(new_source)
                source_cols[id(new_source)] = [c for c in new_source.data]
        for plot in plots:
            for hook in plot.hooks:
                hook(plot, plot.current_frame)
            for callback in plot.callbacks:
                callback.initialize(plot_id=self.id)
        self.handles['shared_sources'] = shared_sources
        self.handles['source_cols'] = source_cols

    def init_links(self):
        links = LinkCallback.find_links(self)
        callbacks = []
        for link, src_plot, tgt_plot in links:
            cb = Link._callbacks['bokeh'][type(link)]
            if src_plot is None or (link._requires_target and tgt_plot is None):
                continue
            # The link callback (`cb`) is instantiated (with side-effects).
            callbacks.append(cb(self.root, link, src_plot, tgt_plot))
        return callbacks


class CompositePlot(BokehPlot):
    """CompositePlot is an abstract baseclass for plot types that draw
    render multiple axes. It implements methods to add an overall title
    to such a plot.

    """

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

    fontsize = param.Parameter(default={'title': '15pt'}, allow_None=True,  doc="""
       Specifies various fontsizes of the displayed text.

       Finer control is available by supplying a dictionary where any
       unmentioned keys reverts to the default sizes, e.g:

          {'title': '15pt'}""")

    def _link_dimensioned_streams(self):
        """Should perform any linking required to update titles when dimensioned
        streams change.

        """
        streams = [s for s in self.streams if any(k in self.dimensions for k in s.contents)]
        for s in streams:
            s.add_subscriber(self._stream_update, 1)

    def _stream_update(self, **kwargs):
        contents = [k for s in self.streams for k in s.contents]
        key = tuple(None if d in contents else k for d, k in zip(self.dimensions, self.current_key, strict=None))
        key = wrap_tuple_streams(key, self.dimensions, self.streams)
        self._get_title_div(key)

    @property
    def current_handles(self):
        """Should return a list of plot objects that have changed and
        should be updated.

        """
        return [self.handles['title']] if 'title' in self.handles else []



class GridPlot(CompositePlot, GenericCompositePlot):
    """Plot a group of elements in a grid layout based on a GridSpace element
    object.

    """

    axis_offset = param.Integer(default=50, doc="""
        Number of pixels to adjust row and column widths and height by
        to compensate for shared axes.""")

    fontsize = param.Parameter(default={'title': '16pt'},
                               allow_None=True,  doc="""
       Specifies various fontsizes of the displayed text.

       Finer control is available by supplying a dictionary where any
       unmentioned keys reverts to the default sizes, e.g:

          {'title': '15pt'}""")

    merge_tools = param.Boolean(default=True, doc="""
        Whether to merge all the tools into a single toolbar""")

    shared_xaxis = param.Boolean(default=False, doc="""
        If enabled the x-axes of the GridSpace will be drawn from the
        objects inside the Grid rather than the GridSpace dimensions.""")

    shared_yaxis = param.Boolean(default=False, doc="""
        If enabled the x-axes of the GridSpace will be drawn from the
        objects inside the Grid rather than the GridSpace dimensions.""")

    show_legend = param.Boolean(default=False, doc="""
        Adds a legend based on the entries of the middle-right plot""")

    xaxis = param.Selector(default=True,
                                 objects=['bottom', 'top', None, True, False], doc="""
        Whether and where to display the xaxis, supported options are
        'bottom', 'top' and None.""")

    yaxis = param.Selector(default=True,
                                 objects=['left', 'right', None, True, False], doc="""
        Whether and where to display the yaxis, supported options are
        'left', 'right' and None.""")

    xrotation = param.Integer(default=0, bounds=(0, 360), doc="""
        Rotation angle of the xticks.""")

    yrotation = param.Integer(default=0, bounds=(0, 360), doc="""
        Rotation angle of the yticks.""")

    plot_size = param.ClassSelector(default=120, class_=(int, tuple), doc="""
        Defines the width and height of each plot in the grid, either
        as a tuple specifying width and height or an integer for a
        square plot.""")

    sync_legends = param.Boolean(default=True, doc="""
        Whether to sync the legend when muted/unmuted based on the name""")

    def __init__(self, layout, ranges=None, layout_num=1, keys=None, **params):
        if not isinstance(layout, GridSpace):
            raise Exception("GridPlot only accepts GridSpace.")
        super().__init__(layout=layout, layout_num=layout_num,
                                       ranges=ranges, keys=keys, **params)
        self.cols, self.rows = layout.shape
        self.subplots, self.layout = self._create_subplots(layout, ranges)
        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])
        if 'axis_offset' in params:
            self.param.warning("GridPlot axis_offset option is deprecated "
                               "since 1.12.0 since subplots are now sized "
                               "correctly and therefore no longer require "
                               "an offset.")


    def _create_subplots(self, layout, ranges):
        if isinstance(self.plot_size, tuple):
            width, height = self.plot_size
        else:
            width, height = self.plot_size, self.plot_size

        subplots = {}
        frame_ranges = self.compute_ranges(layout, None, ranges)
        keys = self.keys[:1] if self.dynamic else self.keys
        frame_ranges = dict([(key, self.compute_ranges(layout, key, frame_ranges))
                                    for key in keys])
        collapsed_layout = layout.clone(shared_data=False, id=layout.id)
        for i, coord in enumerate(layout.keys(full_grid=True)):
            r = i % self.rows
            c = i // self.rows

            if not isinstance(coord, tuple): coord = (coord,)
            view = layout.data.get(coord, None)
            # Create subplot
            if view is not None:
                vtype = view.type if isinstance(view, HoloMap) else view.__class__
                opts = self.lookup_options(view, 'plot').options
            else:
                vtype = None

            if type(view) in (Layout, NdLayout):
                raise SkipRendering("Cannot plot nested Layouts.")
            if not displayable(view):
                view = collate(view)

            # Create axes
            kwargs = {}
            if width is not None:
                kwargs['frame_width'] = width
            if height is not None:
                kwargs['frame_height'] = height
            if c == 0:
                kwargs['align'] = 'end'
            if c == 0 and r != 0:
                kwargs['xaxis'] = None
            if c != 0 and r == 0:
                kwargs['yaxis'] = None
            if r != 0 and c != 0:
                kwargs['xaxis'] = None
                kwargs['yaxis'] = None

            if 'border' not in kwargs:
                kwargs['border'] = 3

            if self.show_legend and c == (self.cols-1) and r == (self.rows-1):
                kwargs['show_legend'] = True
                kwargs['legend_position'] = 'right'
            else:
                kwargs['show_legend'] = False

            if not self.shared_xaxis:
                kwargs['xaxis'] = None

            if not self.shared_yaxis:
                kwargs['yaxis'] = None

            # Create subplot
            plotting_class = Store.registry[self.renderer.backend].get(vtype, None)
            if plotting_class is None:
                if view is not None:
                    self.param.warning(
                        f"Bokeh plotting class for {vtype.__name__} type not found, "
                        "object will not be rendered.")
            else:
                subplot = plotting_class(view, dimensions=self.dimensions,
                                         show_title=False, subplot=True,
                                         renderer=self.renderer, root=self.root,
                                         ranges=frame_ranges, uniform=self.uniform,
                                         keys=self.keys, **dict(opts, **kwargs))
                collapsed_layout[coord] = (subplot.layout
                                           if isinstance(subplot, GenericCompositePlot)
                                           else subplot.hmap)
                subplots[coord] = subplot
        return subplots, collapsed_layout


    def initialize_plot(self, ranges=None, plots=None):
        if plots is None:
            plots = []
        ranges = self.compute_ranges(self.layout, self.keys[-1], None)
        passed_plots = list(plots)
        plots = [[None for c in range(self.cols)] for r in range(self.rows)]
        for i, coord in enumerate(self.layout.keys(full_grid=True)):
            r = i % self.rows
            c = i // self.rows
            subplot = self.subplots.get(wrap_tuple(coord), None)
            if subplot is not None:
                plot = subplot.initialize_plot(ranges=ranges, plots=passed_plots)
                plots[r][c] = plot
                passed_plots.append(plot)
            else:
                passed_plots.append(None)

        plot = gridplot(plots[::-1],
                        merge_tools=False,
                        sizing_mode=self.sizing_mode,
                        toolbar_location=self.toolbar)
        if self.sync_legends:
            sync_legends(plot)
        plot = self._make_axes(plot)
        if hasattr(plot, "toolbar") and self.merge_tools:
            plot.toolbar = merge_tools(plots, hide_toolbar=True)
        title = self._get_title_div(self.keys[-1])
        if title:
            plot = Column(title, plot)
            self.handles['title'] = title

        self.handles['plot'] = plot
        self.handles['plots'] = plots

        if self.shared_datasource:
            self.sync_sources()

        if self.top_level:
            self.init_links()

        self.drawn = True

        return self.handles['plot']


    def _make_axes(self, plot):
        width, height = self.renderer.get_size(plot)
        x_axis, y_axis = None, None
        keys = self.layout.keys(full_grid=True)
        if self.xaxis:
            flip = self.shared_xaxis
            rotation = self.xrotation
            lsize = self._fontsize('xlabel').get('fontsize')
            tsize = self._fontsize('xticks', common=False).get('fontsize')
            xfactors = list(unique_iterator([wrap_tuple(k)[0] for k in keys]))
            x_axis = make_axis('x', width, xfactors, self.layout.kdims[0],
                               flip=flip, rotation=rotation, label_size=lsize,
                               tick_size=tsize)
        if self.yaxis and self.layout.ndims > 1:
            flip = self.shared_yaxis
            rotation = self.yrotation
            lsize = self._fontsize('ylabel').get('fontsize')
            tsize = self._fontsize('yticks', common=False).get('fontsize')
            yfactors = list(unique_iterator([k[1] for k in keys]))
            y_axis = make_axis('y', height, yfactors, self.layout.kdims[1],
                               flip=flip, rotation=rotation, label_size=lsize,
                               tick_size=tsize)
        if x_axis and y_axis:
            plot = filter_toolboxes(plot)
            r1, r2 = ([y_axis, plot], [None, x_axis])
            if self.shared_xaxis:
                r1, r2 = r2, r1
            if self.shared_yaxis:
                x_axis.margin = (0, 0, 0, 50)
                r1, r2 = r1[::-1], r2[::-1]
            plot = gridplot([r1, r2], merge_tools=False)
            if self.merge_tools:
                plot.toolbar = merge_tools([r1, r2], autohide=self.autohide_toolbar)
        elif y_axis:
            models = [y_axis, plot]
            if self.shared_yaxis: models = models[::-1]
            plot = Row(*models)
        elif x_axis:
            models = [plot, x_axis]
            if self.shared_xaxis: models = models[::-1]
            plot = Column(*models)
        return plot


    @update_shared_sources
    def update_frame(self, key, ranges=None):
        """Update the internal state of the Plot to represent the given
        key tuple (where integers represent frames). Returns this
        state.

        """
        ranges = self.compute_ranges(self.layout, key, ranges)
        for coord in self.layout.keys(full_grid=True):
            subplot = self.subplots.get(wrap_tuple(coord), None)
            if subplot is not None:
                subplot.update_frame(key, ranges)
        title = self._get_title_div(key)
        if title:
            self.handles['title']



class LayoutPlot(CompositePlot, GenericLayoutPlot):

    shared_axes = param.Boolean(default=True, doc="""
        Whether axes should be shared across plots""")

    shared_datasource = param.Boolean(default=False, doc="""
        Whether Elements drawing the data from the same object should
        share their Bokeh data source allowing for linked brushing
        and other linked behaviors.""")

    merge_tools = param.Boolean(default=True, doc="""
        Whether to merge all the tools into a single toolbar""")

    sync_legends = param.Boolean(default=True, doc="""
        Whether to sync the legend when muted/unmuted based on the name""")

    show_legends = param.ClassSelector(default=None, class_=(list, int, bool), doc="""
        Whether to show the legend for a particular subplot by index. If True all legends
        will be shown. If False no legends will be shown.""")

    legend_position = param.Selector(objects=["top_right",
                                                    "top_left",
                                                    "bottom_left",
                                                    "bottom_right",
                                                    'right', 'left',
                                                    'top', 'bottom'],
                                                    default="top_right",
                                                    doc="""
        Allows selecting between a number of predefined legend position
        options. Will only be applied if show_legend is not None.""")

    tabs = param.Boolean(default=False, doc="""
        Whether to display overlaid plots in separate panes""")

    def __init__(self, layout, keys=None, **params):
        super().__init__(layout, keys=keys, **params)
        self.layout, self.subplots, self.paths = self._init_layout(layout)
        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])

    @param.depends('show_legends', 'legend_position', watch=True, on_init=True)
    def _update_show_legend(self):
        if self.show_legends is not None:
            select_legends(self.layout, self.show_legends, self.legend_position)

    def _init_layout(self, layout):
        # Situate all the Layouts in the grid and compute the gridspec
        # indices for all the axes required by each LayoutPlot.
        layout_count = 0
        collapsed_layout = layout.clone(shared_data=False, id=layout.id)
        frame_ranges = self.compute_ranges(layout, None, None)
        keys = self.keys[:1] if self.dynamic else self.keys
        frame_ranges = dict([(key, self.compute_ranges(layout, key, frame_ranges))
                                    for key in keys])
        layout_items = layout.grid_items()
        layout_dimensions = layout.kdims if isinstance(layout, NdLayout) else None
        layout_subplots, layouts, paths = {}, {}, {}
        for r, c in self.coords:
            # Get view at layout position and wrap in AdjointLayout
            key, view = layout_items.get((c, r) if self.transpose else (r, c), (None, None))
            view = view if isinstance(view, AdjointLayout) else AdjointLayout([view])
            layouts[(r, c)] = view
            paths[r, c] = key

            # Compute the layout type from shape
            layout_lens = {1:'Single', 2:'Dual', 3: 'Triple'}
            layout_type = layout_lens.get(len(view), 'Single')

            # Get the AdjoinLayout at the specified coordinate
            positions = AdjointLayoutPlot.layout_dict[layout_type]['positions']

            # Create temporary subplots to get projections types
            # to create the correct subaxes for all plots in the layout
            layout_key, _ = layout_items.get((r, c), (None, None))
            if isinstance(layout, NdLayout) and layout_key:
                layout_dimensions = dict(zip(layout_dimensions, layout_key, strict=None))

            # Generate the axes and create the subplots with the appropriate
            # axis objects, handling any Empty objects.
            empty = isinstance(view.main, Empty)
            if empty or view.main is None:
                continue
            elif not view.traverse(lambda x: x, [Element]):
                self.param.warning(f'{view.main} is empty, skipping subplot.')
                continue
            else:
                layout_count += 1
            num = 0 if empty else layout_count
            subplots, adjoint_layout = self._create_subplots(
                view, positions, layout_dimensions, frame_ranges, num=num
            )

            # Generate the AdjointLayoutsPlot which will coordinate
            # plotting of AdjointLayouts in the larger grid
            plotopts = self.lookup_options(view, 'plot').options
            layout_plot = AdjointLayoutPlot(adjoint_layout, layout_type, subplots, **plotopts)
            layout_subplots[(r, c)] = layout_plot
            if layout_key:
                collapsed_layout[layout_key] = adjoint_layout
        return collapsed_layout, layout_subplots, paths


    def _create_subplots(self, layout, positions, layout_dimensions, ranges, num=0):
        """Plot all the views contained in the AdjointLayout Object using axes
        appropriate to the layout configuration. All the axes are
        supplied by LayoutPlot - the purpose of the call is to
        invoke subplots with correct options and styles and hide any
        empty axes as necessary.

        """
        subplots = {}
        adjoint_clone = layout.clone(shared_data=False, id=layout.id)
        main_plot = None
        for pos in positions:
            # Pos will be one of 'main', 'top' or 'right' or None
            element = layout.get(pos, None)
            if element is None or not element.traverse(lambda x: x, [Element, Empty]):
                continue
            if not displayable(element):
                element = collate(element)

            subplot_opts = dict(adjoined=main_plot)
            # Options common for any subplot
            vtype = element.type if isinstance(element, HoloMap) else element.__class__
            plot_type = Store.registry[self.renderer.backend].get(vtype, None)
            plotopts = self.lookup_options(element, 'plot').options
            side_opts = {}
            if pos != 'main':
                plot_type = AdjointLayoutPlot.registry.get(vtype, plot_type)
                if pos == 'right':
                    yaxis = 'right-bare' if plot_type and 'bare' in plot_type.yaxis else 'right'
                    width = plot_type.width if plot_type else 0
                    side_opts = dict(height=main_plot.height, yaxis=yaxis,
                                     width=width, invert_axes=True,
                                     labelled=['y'], xticks=1, xaxis=main_plot.xaxis)
                else:
                    xaxis = 'top-bare' if plot_type and 'bare' in plot_type.xaxis else 'top'
                    height = plot_type.height if plot_type else 0
                    side_opts = dict(width=main_plot.width, xaxis=xaxis,
                                     height=height, labelled=['x'],
                                     yticks=1, yaxis=main_plot.yaxis)

            # Override the plotopts as required
            # Customize plotopts depending on position.
            plotopts = dict(side_opts, **plotopts)
            plotopts.update(subplot_opts)

            if vtype is Empty:
                adjoint_clone[pos] = element
                subplots[pos] = None
                continue
            elif plot_type is None:
                self.param.warning(
                    f"Bokeh plotting class for {vtype.__name__} type not found, object "
                    " will not be rendered.")
                continue
            num = num if len(self.coords) > 1 else 0
            subplot = plot_type(element, keys=self.keys,
                                dimensions=self.dimensions,
                                layout_dimensions=layout_dimensions,
                                ranges=ranges, subplot=True, root=self.root,
                                uniform=self.uniform, layout_num=num,
                                renderer=self.renderer,
                                **dict({'shared_axes': self.shared_axes},
                                       **plotopts))
            subplots[pos] = subplot
            if isinstance(plot_type, type) and issubclass(plot_type, GenericCompositePlot):
                adjoint_clone[pos] = subplots[pos].layout
            else:
                adjoint_clone[pos] = subplots[pos].hmap
            if pos == 'main':
                main_plot = subplot

        return subplots, adjoint_clone


    def _compute_grid(self):
        """Computes an empty grid to position the plots on by expanding
        any AdjointLayouts into multiple rows and columns.

        """
        widths = []
        for c in range(self.cols):
            c_widths = []
            for r in range(self.rows):
                subplot = self.subplots.get((r, c), None)
                nsubplots = 1 if subplot is None else len(subplot.layout)
                c_widths.append(2 if nsubplots > 1 else 1)
            widths.append(max(c_widths))

        heights = []
        for r in range(self.rows):
            r_heights = []
            for c in range(self.cols):
                subplot = self.subplots.get((r, c), None)
                nsubplots = 1 if subplot is None else len(subplot.layout)
                r_heights.append(2 if nsubplots > 2 else 1)
            heights.append(max(r_heights))

        # Generate empty grid
        rows = sum(heights)
        cols = sum(widths)
        grid = [[None]*cols for _ in range(rows)]

        return grid


    def initialize_plot(self, plots=None, ranges=None):
        ranges = self.compute_ranges(self.layout, self.keys[-1], None)
        opts = self.layout.opts.get('plot', self.backend)
        opts = {} if opts is None else opts.kwargs

        plot_grid = self._compute_grid()
        passed_plots = [] if plots is None else plots
        r_offset = 0
        col_offsets = defaultdict(int)
        tab_plots = []

        stretch_width = False
        stretch_height = False
        for r in range(self.rows):
            # Compute row offset
            row = [(k, sp) for k, sp in self.subplots.items() if k[0] == r]
            row_padded = any(len(sp.layout) > 2 for k, sp in row)
            if row_padded:
                r_offset += 1

            for c in range(self.cols):
                subplot = self.subplots.get((r, c), None)

                # Compute column offset
                col = [(k, sp) for k, sp in self.subplots.items() if k[1] == c]
                col_padded = any(len(sp.layout) > 1 for k, sp in col)
                if col_padded:
                    col_offsets[r] += 1
                c_offset = col_offsets.get(r, 0)

                if subplot is None:
                    continue

                shared_plots = list(passed_plots) if self.shared_axes else None
                subplots = subplot.initialize_plot(ranges=ranges, plots=shared_plots)
                nsubplots = len(subplots)

                modes = {sp.sizing_mode for sp in subplots
                         if sp.sizing_mode not in (None, 'auto', 'fixed')}
                sizing_mode = self.sizing_mode
                if modes:
                    responsive_width = any(s in m for m in modes for s in ('width', 'both'))
                    responsive_height = any(s in m for m in modes for s in ('height', 'both'))
                    stretch_width |= responsive_width
                    stretch_height |= responsive_height
                    if responsive_width and responsive_height:
                        sizing_mode = 'stretch_both'
                    elif responsive_width:
                        sizing_mode = 'stretch_width'
                    elif responsive_height:
                        sizing_mode = 'stretch_height'

                # If tabs enabled lay out AdjointLayout on grid
                if self.tabs:
                    title = subplot.subplots['main']._format_title(self.keys[-1],
                                                                   dimensions=False)

                    if not title:
                        title = ' '.join(self.paths[r,c])

                    if nsubplots == 1:
                        grid = subplots[0]
                    else:
                        children = [subplots] if nsubplots == 2 else [[subplots[2], None], subplots[:2]]
                        grid = gridplot(children,
                                        merge_tools=False,
                                        toolbar_location=self.toolbar,
                                        sizing_mode=sizing_mode)
                        if self.merge_tools:
                            grid.toolbar = merge_tools(children, autohide=self.autohide_toolbar)
                    tab_plots.append((title, grid))
                    continue

                # Situate plot in overall grid
                if nsubplots > 2:
                    plot_grid[r+r_offset-1][c+c_offset-1] = subplots[2]
                plot_column = plot_grid[r+r_offset]
                if nsubplots > 1:
                    plot_column[c+c_offset-1] = subplots[0]
                    plot_column[c+c_offset] = subplots[1]
                else:
                    plot_column[c+c_offset-int(col_padded)] = subplots[0]
                passed_plots.append(subplots[0])

        if 'sizing_mode' in opts:
            sizing_mode = opts['sizing_mode']
        elif stretch_width and stretch_height:
            sizing_mode = 'stretch_both'
        elif stretch_width:
            sizing_mode = 'stretch_width'
        elif stretch_height:
            sizing_mode = 'stretch_height'
        else:
            sizing_mode = None

        # Wrap in appropriate layout model
        if self.tabs:
            plots = filter_toolboxes([p for t, p in tab_plots])
            panels = [TabPanel(child=child, title=t) for t, child in tab_plots]
            layout_plot = Tabs(tabs=panels, sizing_mode=sizing_mode)
        else:
            plot_grid = filter_toolboxes(plot_grid)
            layout_plot = gridplot(
                children=plot_grid,
                toolbar_location=self.toolbar,
                merge_tools=False,
                sizing_mode=sizing_mode
            )
            if self.sync_legends:
                sync_legends(layout_plot)
            if self.merge_tools:
                layout_plot.toolbar = merge_tools(plot_grid, autohide=self.autohide_toolbar)

        title = self._get_title_div(self.keys[-1])
        if title:
            self.handles['title'] = title
            layout_plot = Column(title, layout_plot, sizing_mode=sizing_mode)

        self.handles['plot'] = layout_plot
        self.handles['plots'] = plots

        if self.shared_datasource:
            self.sync_sources()

        if self.top_level:
            self.init_links()

        self.drawn = True

        return self.handles['plot']

    @update_shared_sources
    def update_frame(self, key, ranges=None):
        """Update the internal state of the Plot to represent the given
        key tuple (where integers represent frames). Returns this
        state.

        """
        ranges = self.compute_ranges(self.layout, key, ranges)
        for r, c in self.coords:
            subplot = self.subplots.get((r, c), None)
            if subplot is not None:
                subplot.update_frame(key, ranges)
        title = self._get_title_div(key)
        if title:
            self.handles['title'] = title



class AdjointLayoutPlot(BokehPlot, GenericAdjointLayoutPlot):

    registry = {}

    def __init__(self, layout, layout_type, subplots, **params):
        # The AdjointLayout ViewableElement object
        self.layout = layout
        # Type may be set to 'Embedded Dual' by a call it grid_situate
        self.layout_type = layout_type
        self.view_positions = self.layout_dict[self.layout_type]['positions']

        # The supplied (axes, view) objects as indexed by position
        super().__init__(subplots=subplots, **params)

    def initialize_plot(self, ranges=None, plots=None):
        """Plot all the views contained in the AdjointLayout Object using axes
        appropriate to the layout configuration. All the axes are
        supplied by LayoutPlot - the purpose of the call is to
        invoke subplots with correct options and styles and hide any
        empty axes as necessary.

        """
        if plots is None:
            plots = []
        if plots is None: plots = []
        adjoined_plots = []
        for pos in self.view_positions:
            # Pos will be one of 'main', 'top' or 'right' or None
            subplot = self.subplots.get(pos, None)
            # If no view object or empty position, disable the axis
            if subplot is None:
                adjoined_plots.append(empty_plot(0, 0))
            else:
                passed_plots = plots + adjoined_plots
                adjoined_plots.append(subplot.initialize_plot(ranges=ranges, plots=passed_plots))
        self.drawn = True
        return adjoined_plots or [None]

    def update_frame(self, key, ranges=None):
        plot = None
        for pos in ['main', 'right', 'top']:
            subplot = self.subplots.get(pos)
            if subplot is not None:
                plot = subplot.update_frame(key, ranges)
        return plot
