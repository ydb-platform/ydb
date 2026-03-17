import param

from holoviews.plotting.util import attach_streams

from ...core import AdjointLayout, Empty, GridMatrix, GridSpace, HoloMap, NdLayout
from ...core.options import Store
from ...core.util import wrap_tuple
from ...element import Histogram
from ..plot import (
    CallbackPlot,
    DimensionedPlot,
    GenericAdjointLayoutPlot,
    GenericCompositePlot,
    GenericElementPlot,
    GenericLayoutPlot,
)
from .util import configure_matching_axes_from_dims, figure_grid


class PlotlyPlot(DimensionedPlot, CallbackPlot):

    backend = 'plotly'

    width = param.Integer(default=400)

    height = param.Integer(default=400)

    unsupported_geo_style_opts = []

    @property
    def state(self):
        """The plotting state that gets updated via the update method and
        used by the renderer to generate output.

        """
        return self.handles['fig']


    def _trigger_refresh(self, key):
        """Triggers update to a plot on a refresh event

        """
        if self.top_level:
            self.update(key)
        else:
            self.current_key = None
            self.current_frame = None


    def initialize_plot(self, ranges=None, is_geo=False):
        return self.generate_plot(self.keys[-1], ranges, is_geo=is_geo)


    def update_frame(self, key, ranges=None, is_geo=False):
        return self.generate_plot(key, ranges, is_geo=is_geo)



class LayoutPlot(PlotlyPlot, GenericLayoutPlot):

    hspacing = param.Number(default=120, bounds=(0, None))

    vspacing = param.Number(default=100, bounds=(0, None))

    adjoint_spacing = param.Number(default=20, bounds=(0, None))

    shared_axes = param.Boolean(default=True, doc="""
        Whether axes ranges should be shared across the layout, if
        disabled switches axiswise normalization option on globally.""")

    def __init__(self, layout, **params):
        super().__init__(layout, **params)
        self.layout, self.subplots, self.paths = self._init_layout(layout)

        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])

    def _get_size(self):
        rows, cols = self.layout.shape
        return cols*self.width*0.8, rows*self.height

    def _init_layout(self, layout):
        # Situate all the Layouts in the grid and compute the gridspec
        # indices for all the axes required by each LayoutPlot.
        layout_count = 0
        collapsed_layout = layout.clone(shared_data=False, id=layout.id)
        frame_ranges = self.compute_ranges(layout, None, None)
        frame_ranges = dict([(key, self.compute_ranges(layout, key, frame_ranges))
                                    for key in self.keys])
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
            obj = layouts[(r, c)]
            empty = isinstance(obj.main, Empty)
            if empty:
                obj = AdjointLayout([])
            else:
                layout_count += 1
            subplot_data = self._create_subplots(obj, positions,
                                                 layout_dimensions, frame_ranges,
                                                 num=0 if empty else layout_count)
            subplots, adjoint_layout = subplot_data

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
        subplot_opts = dict(adjoined=layout)
        main_plot = None
        for pos in positions:
            # Pos will be one of 'main', 'top' or 'right' or None
            element = layout.get(pos, None)
            if element is None:
                continue

            # Options common for any subplot
            vtype = element.type if isinstance(element, HoloMap) else element.__class__
            plot_type = Store.registry[self.renderer.backend].get(vtype, None)
            plotopts = self.lookup_options(element, 'plot').options
            side_opts = {}
            if pos != 'main':
                plot_type = AdjointLayoutPlot.registry.get(vtype, plot_type)
                if pos == 'right':
                    side_opts = dict(height=main_plot.height, yaxis='right',
                                     invert_axes=True, width=120, labelled=['y'],
                                     xticks=2, show_title=False)
                else:
                    side_opts = dict(width=main_plot.width, xaxis='top',
                                     height=120, labelled=['x'], yticks=2,
                                     show_title=False)

            # Override the plotopts as required
            # Customize plotopts depending on position.
            plotopts = dict(side_opts, **plotopts)
            plotopts.update(subplot_opts)

            if plot_type is None:
                self.param.warning(
                    f"Plotly plotting class for {vtype.__name__} type not found, "
                    "object will not be rendered.")
                continue
            num = num if len(self.coords) > 1 else 0
            subplot = plot_type(element, keys=self.keys,
                                dimensions=self.dimensions,
                                layout_dimensions=layout_dimensions,
                                ranges=ranges, subplot=True,
                                uniform=self.uniform, layout_num=num,
                                **plotopts)
            subplots[pos] = subplot
            if isinstance(plot_type, type) and issubclass(plot_type, GenericCompositePlot):
                adjoint_clone[pos] = subplots[pos].layout
            else:
                adjoint_clone[pos] = subplots[pos].hmap
            if pos == 'main':
                main_plot = subplot

        return subplots, adjoint_clone


    def generate_plot(self, key, ranges=None, is_geo=False):
        ranges = self.compute_ranges(self.layout, self.keys[-1], None)
        plots = [[] for i in range(self.rows)]
        insert_rows = []
        for r, c in self.coords:
            subplot = self.subplots.get((r, c), None)
            if subplot is not None:
                subplots = subplot.generate_plot(key, ranges=ranges, is_geo=is_geo)

                # Computes plotting offsets depending on
                # number of adjoined plots
                offset = sum(r >= ir for ir in insert_rows)
                if len(subplots) > 2:
                    subplot = figure_grid([[subplots[0], subplots[1]],
                                           [subplots[2], None]],
                                          column_spacing=self.adjoint_spacing,
                                          row_spacing=self.adjoint_spacing)
                elif len(subplots) > 1:
                    subplot = figure_grid([subplots],
                                          column_spacing=self.adjoint_spacing,
                                          row_spacing=self.adjoint_spacing)
                else:
                    subplot = subplots[0]

                plots[r + offset] += [subplot]

        fig = figure_grid(
            list(reversed(plots)),
            column_spacing=self.hspacing,
            row_spacing=self.vspacing
        )

        # Configure axis matching
        if self.shared_axes:
            configure_matching_axes_from_dims(fig)

        fig['layout'].update(title=self._format_title(key))

        self.drawn = True

        self.handles['fig'] = fig
        return self.handles['fig']



class AdjointLayoutPlot(PlotlyPlot, GenericAdjointLayoutPlot):

    registry = {}

    def __init__(self, layout, layout_type, subplots, **params):
        # The AdjointLayout ViewableElement object
        self.layout = layout
        # Type may be set to 'Embedded Dual' by a call it grid_situate
        self.layout_type = layout_type
        self.view_positions = self.layout_dict[self.layout_type]['positions']

        # The supplied (axes, view) objects as indexed by position
        super().__init__(subplots=subplots, **params)

    def initialize_plot(self, ranges=None, is_geo=False):
        """Plot all the views contained in the AdjointLayout Object using axes
        appropriate to the layout configuration. All the axes are
        supplied by LayoutPlot - the purpose of the call is to
        invoke subplots with correct options and styles and hide any
        empty axes as necessary.

        """
        return self.generate_plot(self.keys[-1], ranges, is_geo=is_geo)

    def generate_plot(self, key, ranges=None, is_geo=False):
        adjoined_plots = []
        for pos in ['main', 'right', 'top']:
            # Pos will be one of 'main', 'top' or 'right' or None
            subplot = self.subplots.get(pos, None)
            # If no view object or empty position, disable the axis
            if subplot:
                adjoined_plots.append(
                    subplot.generate_plot(key, ranges=ranges, is_geo=is_geo)
                )
        if not adjoined_plots: adjoined_plots = [None]
        return adjoined_plots



class GridPlot(PlotlyPlot, GenericCompositePlot):
    """Plot a group of elements in a grid layout based on a GridSpace element
    object.

    """

    hspacing = param.Number(default=15, bounds=(0, None))

    vspacing = param.Number(default=15, bounds=(0, None))

    shared_axes = param.Boolean(default=True, doc="""
        Whether axes ranges should be shared across the layout, if
        disabled switches axiswise normalization option on globally.""")

    def __init__(self, layout, ranges=None, layout_num=1, **params):
        if not isinstance(layout, GridSpace):
            raise Exception("GridPlot only accepts GridSpace.")
        super().__init__(layout=layout, layout_num=layout_num,
                                       ranges=ranges, **params)
        self.cols, self.rows = layout.shape
        self.subplots, self.layout = self._create_subplots(layout, ranges)

        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])


    def _create_subplots(self, layout, ranges):
        subplots = {}
        frame_ranges = self.compute_ranges(layout, None, ranges)
        frame_ranges = dict([(key, self.compute_ranges(layout, key, frame_ranges))
                                    for key in self.keys])
        collapsed_layout = layout.clone(shared_data=False, id=layout.id)
        for coord in layout.keys(full_grid=True):
            if not isinstance(coord, tuple): coord = (coord,)
            view = layout.data.get(coord, None)
            # Create subplot
            if view is not None:
                vtype = view.type if isinstance(view, HoloMap) else view.__class__
                opts = self.lookup_options(view, 'plot').options
            else:
                vtype = None

            # Create axes
            kwargs = {}
            if isinstance(layout, GridMatrix):
                if view.traverse(lambda x: x, [Histogram]):
                    kwargs['shared_axes'] = False

            # Create subplot
            plotting_class = Store.registry[self.renderer.backend].get(vtype, None)
            if plotting_class is None:
                if view is not None:
                    self.param.warning(
                        f"Plotly plotting class for {vtype.__name__} type not found, "
                        "object will not be rendered.")
            else:
                subplot = plotting_class(view, dimensions=self.dimensions,
                                         show_title=False, subplot=True,
                                         ranges=frame_ranges, uniform=self.uniform,
                                         keys=self.keys, **dict(opts, **kwargs))
                collapsed_layout[coord] = (subplot.layout
                                           if isinstance(subplot, GenericCompositePlot)
                                           else subplot.hmap)
                subplots[coord] = subplot
        return subplots, collapsed_layout


    def generate_plot(self, key, ranges=None, is_geo=False):
        ranges = self.compute_ranges(self.layout, self.keys[-1], None)
        plots = [[] for r in range(self.cols)]
        for i, coord in enumerate(self.layout.keys(full_grid=True)):
            r = i % self.cols
            subplot = self.subplots.get(wrap_tuple(coord), None)
            if subplot is not None:
                plot = subplot.initialize_plot(ranges=ranges, is_geo=is_geo)
                plots[r].append(plot)
            else:
                plots[r].append(None)

        # Compute final width/height
        w, h = self._get_size(subplot.width, subplot.height)

        fig = figure_grid(plots,
                          column_spacing=self.hspacing,
                          row_spacing=self.vspacing,
                          share_xaxis=True,
                          share_yaxis=True,
                          width=w,
                          height=h
                          )

        fig['layout'].update(title=self._format_title(key))

        self.drawn = True

        self.handles['fig'] = fig
        return self.handles['fig']


    def _get_size(self, width, height):
        max_dim = max(self.layout.shape)
        # Reduce plot size as GridSpace gets larger
        shape_factor = 1. / max_dim
        # Expand small grids to a sensible viewing size
        expand_factor = 1 + (max_dim - 1) * 0.1
        scale_factor = expand_factor * shape_factor
        cols, rows = self.layout.shape
        return (scale_factor * cols * width,
                scale_factor * rows * height)
