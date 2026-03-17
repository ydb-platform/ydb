"""Public API for all plots supported by HoloViews, regardless of
plotting package or backend. Every plotting classes must be a subclass
of this Plot baseclass.

"""
import uuid
import warnings
from ast import literal_eval
from collections import Counter, defaultdict
from functools import partial
from itertools import groupby, product

import numpy as np
import param
from panel.config import config
from panel.io.document import unlocked
from panel.io.notebook import push
from panel.io.state import state
from pyviz_comms import JupyterComm

from ..core import traversal, util
from ..core.data import Dataset, disable_pipeline
from ..core.element import Element, Element3D
from ..core.layout import Empty, Layout, NdLayout
from ..core.options import Compositor, SkipRendering, Store, lookup_options
from ..core.overlay import CompositeOverlay, NdOverlay, Overlay
from ..core.spaces import DynamicMap, HoloMap
from ..core.util import dtype_kind, isfinite, stream_parameters, unique_iterator
from ..element import Graph, Table
from ..selection import NoOpSelectionDisplay
from ..streams import RangeX, RangeXY, RangeY, Stream
from ..util.transform import dim
from .util import (
    attach_streams,
    compute_overlayable_zorders,
    dim_axis_label,
    dynamic_update,
    get_axis_padding,
    get_dynamic_mode,
    get_minimum_span,
    get_nested_plot_frame,
    get_nested_streams,
    get_plot_frame,
    get_range,
    initialize_unbounded,
    scale_fontsize,
    split_dmap_overlay,
    traverse_setter,
)


class Plot(param.Parameterized):
    """Base class of all Plot classes in HoloViews, designed to be
    general enough to use any plotting package or backend.

    """

    backend = None

    # A list of style options that may be supplied to the plotting
    # call
    style_opts = []
    # Sometimes matplotlib doesn't support the common aliases.
    # Use this list to disable any invalid style options
    _disabled_opts = []

    def __init__(self, renderer=None, root=None, **params):
        params = {k: v for k, v in params.items()
                  if k in self.param}
        super().__init__(**params)
        self.renderer = renderer if renderer else Store.renderers[self.backend].instance()
        self._force = False
        self._comm = None
        self._document = None
        self._root = None
        self._pane = None
        self._triggering = []
        self._trigger = []
        self.set_root(root)


    @property
    def state(self):
        """The plotting state that gets updated via the update method and
        used by the renderer to generate output.

        """
        raise NotImplementedError


    def set_root(self, root):
        """Sets the root model on all subplots.

        """
        if root is None:
            return
        for plot in self.traverse(lambda x: x):
            plot._root = root


    @property
    def root(self):
        if self._root:
            return self._root
        elif 'plot' in self.handles and self.top_level:
            return self.state
        else:
            return None


    @property
    def document(self):
        return self._document

    @document.setter
    def document(self, doc):
        if (doc and hasattr(doc, 'on_session_destroyed') and
            self.root is self.handles.get('plot') and
            not isinstance(self, GenericAdjointLayoutPlot)):
            doc.on_session_destroyed(self._session_destroy)
            if self._document:
                if isinstance(self._document.callbacks._session_destroyed_callbacks, set):
                    self._document.callbacks._session_destroyed_callbacks.discard(self._session_destroy)
                else:
                    self._document.callbacks._session_destroyed_callbacks.pop(self._session_destroy, None)

        self._document = doc
        if self.subplots:
            for plot in self.subplots.values():
                if plot is not None:
                    plot.document = doc

    @property
    def pane(self):
        return self._pane

    @pane.setter
    def pane(self, pane):
        if (config.console_output != 'disable' and self.root and
            self.root.ref['id'] not in state._handles and
            isinstance(self.comm, JupyterComm)):
            from IPython.display import display
            handle = display(display_id=uuid.uuid4().hex)
            state._handles[self.root.ref['id']] = (handle, [])

        self._pane = pane
        if self.subplots:
            for plot in self.subplots.values():
                if plot is not None:
                    plot.pane = pane
                if plot is None or not plot.root:
                    continue
                for cb in getattr(plot, 'callbacks', []):
                    if hasattr(pane, '_on_error') and getattr(cb, 'comm', None):
                        cb.comm._on_error = partial(pane._on_error, plot.root.ref['id'])
        elif self.root:
            for cb in getattr(self, 'callbacks', []):
                if hasattr(pane, '_on_error') and getattr(cb, 'comm', None):
                    cb.comm._on_error = partial(pane._on_error, self.root.ref['id'])

    @property
    def comm(self):
        return self._comm


    @comm.setter
    def comm(self, comm):
        self._comm = comm
        if self.subplots:
            for plot in self.subplots.values():
                if plot is not None:
                    plot.comm = comm


    def initialize_plot(self, ranges=None):
        """Initialize the matplotlib figure.

        """
        raise NotImplementedError


    def update(self, key):
        """Update the internal state of the Plot to represent the given
        key tuple (where integers represent frames). Returns this
        state.

        """
        return self.state


    def cleanup(self):
        """Cleans up references to the plot on the attached Stream
        subscribers.

        """
        plots = self.traverse(lambda x: x, [Plot])
        for plot in plots:
            if not isinstance(plot, (GenericElementPlot, GenericOverlayPlot)):
                continue
            for stream in set(plot.streams):
                stream._subscribers = [
                    (p, subscriber) for p, subscriber in stream._subscribers
                    if not util.is_param_method(subscriber) or
                    util.get_method_owner(subscriber) not in plots
                ]

    def _session_destroy(self, session_context):
        self.cleanup()

    def refresh(self, **kwargs):
        """Refreshes the plot by rerendering it and then pushing
        the updated data if the plot has an associated Comm.

        """
        if self.renderer.mode == 'server' and not state._unblocked(self.document):
            # If we do not have the Document lock, schedule refresh as callback
            self._triggering += [(s, dict(s._metadata)) for p in self.traverse(lambda x: x, [Plot])
                                 for s in getattr(p, 'streams', []) if s._triggering]

            if self.document and self.document.session_context:
                self.document.add_next_tick_callback(self.refresh)
                return

        # Ensure that server based tick callbacks maintain stream triggering state
        for s, metadata in self._triggering:
            s._triggering = True
            s._metadata.update(metadata)
        try:
            traverse_setter(self, '_force', True)
            key = self.current_key if self.current_key else self.keys[0]
            dim_streams = [stream for stream in self.streams
                           if any(c in self.dimensions for c in stream.contents)]
            stream_params = stream_parameters(dim_streams)
            key = tuple(None if d in stream_params else k
                        for d, k in zip(self.dimensions, key, strict=None))
            stream_key = util.wrap_tuple_streams(key, self.dimensions, self.streams)


            self._trigger_refresh(stream_key)
            if self.top_level:
                self.push()
        except Exception as e:
            raise e
        finally:
            # Reset triggering state
            for s, _ in self._triggering:
                s._triggering = False
                s._metadata.clear()
            self._triggering = []


    def _trigger_refresh(self, key):
        """Triggers update to a plot on a refresh event

        """
        # Update if not top-level, batched or an ElementPlot
        if not self.top_level or isinstance(self, GenericElementPlot):
            with unlocked():
                self.update(key)

    def push(self):
        """Pushes plot updates to the frontend.

        """
        root = self._root
        if (root and self.pane is not None and
            root.ref['id'] in self.pane._plots):
            child_pane = self.pane._plots[root.ref['id']][1]
        else:
            child_pane = None

        if self.renderer.backend != 'bokeh' and child_pane is not None:
            child_pane.object = self.renderer.get_plot_state(self)
        elif (self.renderer.mode != 'server' and root and
              'embedded' not in root.tags and self.document and self.comm):
            push(self.document, self.comm)

    @property
    def id(self):
        return self.comm.id if self.comm else id(self.state)

    def __len__(self):
        """Returns the total number of available frames.

        """
        raise NotImplementedError

    @classmethod
    def lookup_options(cls, obj, group):
        return lookup_options(obj, group, cls.backend)



class PlotSelector:
    """Proxy that allows dynamic selection of a plotting class based on a
    function of the plotted object. Behaves like a Plot class and
    presents the same parameterized interface.

    """

    _disabled_opts = []

    def __init__(self, selector, plot_classes, allow_mismatch=False):
        """The selector function accepts a component instance and returns
        the appropriate key to index plot_classes dictionary.

        """
        self.selector = selector
        self.plot_classes = dict(plot_classes)
        interface = self._define_interface(self.plot_classes.values(), allow_mismatch)
        self.style_opts, self.plot_options = interface

    def selection_display(self, obj):
        plt_class = self.get_plot_class(obj)
        return getattr(plt_class, 'selection_display', None)

    def _define_interface(self, plots, allow_mismatch):
        parameters = [{k:v.precedence for k,v in plot.param.objects().items()
                       if ((v.precedence is None) or (v.precedence >= 0))}
                      for plot in plots]
        param_sets = [set(params.keys()) for params in parameters]
        if not allow_mismatch and not all(pset == param_sets[0] for pset in param_sets):
            # Find the mismatching sets
            mismatching_sets = [pset for pset in param_sets if pset != param_sets[0]]

            # Print the mismatching sets
            for mismatch_set in mismatching_sets:
                print("Mismatching plot options:", mismatch_set)

            raise Exception("All selectable plot classes must have identical plot options.")
        styles= [plot.style_opts for plot in plots]

        if not allow_mismatch and not all(style == styles[0] for style in styles):
            raise Exception("All selectable plot classes must have identical style options.")

        plot_params = {p: v for params in parameters for p, v in params.items()}
        return [s for style in styles for s in style], plot_params

    def __call__(self, obj, **kwargs):
        plot_class = self.get_plot_class(obj)
        return plot_class(obj, **kwargs)

    def get_plot_class(self, obj):
        key = self.selector(obj)
        if key not in self.plot_classes:
            msg = "Key %s returned by selector not in set: %s"
            raise Exception(msg  % (key, ', '.join(self.plot_classes.keys())))
        return self.plot_classes[key]

    def __setattr__(self, label, value):
        try:
            return super().__setattr__(label, value)
        except Exception as e:
            plot_cls_str = ', '.join(str(cls) for cls in self.__dict__['plot_classes'].values())
            msg = f"Please set class parameters directly on classes {plot_cls_str}"
            raise Exception(msg) from e

    def params(self):
        return self.plot_options

    @property
    def param(self):
        return self.plot_options



class DimensionedPlot(Plot):
    """DimensionedPlot implements a number of useful methods
    to compute dimension ranges and titles containing the
    dimension values.

    """

    fontsize = param.Parameter(default=None, allow_None=True,  doc="""
       Specifies various font sizes of the displayed text.

       Finer control is available by supplying a dictionary where any
       unmentioned keys revert to the default sizes, e.g:

          {'ticks':20, 'title':15,
           'ylabel':5, 'xlabel':5, 'zlabel':5,
           'legend':8, 'legend_title':13}

       You can set the font size of 'zlabel', 'ylabel' and 'xlabel'
       together using the 'labels' key.""")

    fontscale = param.Number(default=None, doc="""
       Scales the size of all fonts.""")

    #Allowed fontsize keys
    _fontsize_keys = ['xlabel','ylabel', 'zlabel', 'clabel', 'labels',
                      'xticks', 'yticks', 'zticks', 'cticks', 'ticks',
                      'minor_xticks', 'minor_yticks', 'minor_ticks',
                      'title', 'legend', 'legend_title',
                      ]

    show_title = param.Boolean(default=True, doc="""
        Whether to display the plot title.""")

    title = param.String(default="{label} {group}\n{dimensions}", doc="""
        The formatting string for the title of this plot, allows defining
        a label group separator and dimension labels.""")

    normalize = param.Boolean(default=True, doc="""
        Whether to compute ranges across all Elements at this level
        of plotting. Allows selecting normalization at different levels
        for nested data containers.""")

    projection = param.Parameter(default=None, doc="""
        Allows supplying a custom projection to transform the axis
        coordinates during display. Example projections include '3d'
        and 'polar' projections supported by some backends. Depending
        on the backend custom, projection objects may be supplied.""")

    def __init__(self, keys=None, dimensions=None, layout_dimensions=None,
                 uniform=True, subplot=False, adjoined=None, layout_num=0,
                 style=None, subplots=None, dynamic=False, **params):
        self.subplots = subplots
        self.adjoined = adjoined
        self.dimensions = dimensions
        self.layout_num = layout_num
        self.layout_dimensions = layout_dimensions
        self.subplot = subplot
        self.keys = keys if keys is None else list(keys)
        self.uniform = uniform
        self.dynamic = dynamic
        self.drawn = False
        self.handles = {}
        self.group = None
        self.label = None
        self.current_frame = None
        self.current_key = None
        self.ranges = {}
        self._updated = False # Whether the plot should be marked as updated
        super().__init__(**params)

    def __getitem__(self, frame):
        """Get the state of the Plot for a given frame number.

        """
        if isinstance(frame, int) and frame > len(self):
            self.param.warning(f"Showing last frame available: {len(self)}")
        if not self.drawn: self.handles['fig'] = self.initialize_plot()
        if not isinstance(frame, tuple):
            frame = self.keys[frame]
        self.update_frame(frame)
        return self.state

    def _get_frame(self, key):
        """Required on each MPLPlot type to get the data corresponding
        just to the current frame out from the object.

        """

    def matches(self, spec):
        """Matches a specification against the current Plot.

        """
        if callable(spec) and not isinstance(spec, type): return spec(self)
        elif isinstance(spec, type): return isinstance(self, spec)
        else:
            raise ValueError("Matching specs have to be either a type or a callable.")


    def traverse(self, fn=None, specs=None, full_breadth=True):
        """Traverses any nested DimensionedPlot returning a list
        of all plots that match the specs. The specs should
        be supplied as a list of either Plot types or callables,
        which should return a boolean given the plot class.

        """
        accumulator = []
        matches = specs is None
        if not matches:
            for spec in specs:
                matches = self.matches(spec)
                if matches: break
        if matches:
            accumulator.append(fn(self) if fn else self)

        # Assumes composite objects are iterables
        if hasattr(self, 'subplots') and self.subplots:
            for el in self.subplots.values():
                if el is None:
                    continue
                accumulator += el.traverse(fn, specs, full_breadth)
                if not full_breadth: break
        return accumulator

    def _frame_title(self, key, group_size=2, separator='\n'):
        """Returns the formatted dimension group strings
        for a particular frame.

        """
        if self.layout_dimensions is not None:
            dimensions, key = zip(*self.layout_dimensions.items(), strict=None)
        elif not self.dynamic and (not self.uniform or len(self) == 1) or self.subplot:
            return ''
        else:
            key = key if isinstance(key, tuple) else (key,)
            dimensions = self.dimensions
        dimension_labels = [dim.pprint_value_string(k) for dim, k in
                            zip(dimensions, key, strict=None)]
        groups = [', '.join(dimension_labels[i*group_size:(i+1)*group_size])
                  for i in range(len(dimension_labels))]
        return util.bytes_to_unicode(separator.join(g for g in groups if g))

    def _format_title(self, key, dimensions=True, separator='\n'):
        label, group, type_name, dim_title = self._format_title_components(
            key, dimensions=True, separator='\n'
        )
        title = util.bytes_to_unicode(self.title).format(
            label=util.bytes_to_unicode(label),
            group=util.bytes_to_unicode(group),
            type=type_name,
            dimensions=dim_title
        )
        return title.strip(' \n')

    def _format_title_components(self, key, dimensions=True, separator='\n'):
        """Determine components of title as used by _format_title method.

        To be overridden in child classes.

        Return signature: (label, group, type_name, dim_title)

        """
        return (self.label, self.group, type(self).__name__, '')

    def _get_fontsize_defaults(self):
        """Should returns default fontsize for the following keywords:

            * ticks
            * minor_ticks
            * label
            * title
            * legend
            * legend_title

        However may also provide more specific defaults for
        specific axis label or ticks, e.g. clabel or xticks.

        """
        return {}

    def _fontsize(self, key, label='fontsize', common=True):
        if not self.fontsize and not self.fontscale:
            return {}
        elif not isinstance(self.fontsize, dict) and self.fontsize is not None and common:
            return {label: scale_fontsize(self.fontsize, self.fontscale)}

        fontsize = self.fontsize if isinstance(self.fontsize, dict) else {}
        unknown_keys = set(fontsize.keys()) - set(self._fontsize_keys)
        if unknown_keys:
            msg = "Popping unknown keys %r from fontsize dictionary.\nValid keys: %r"
            self.param.warning(msg %  (list(unknown_keys), self._fontsize_keys))
            for unknown_key in unknown_keys: fontsize.pop(unknown_key, None)

        defaults = self._get_fontsize_defaults()
        size = None
        if key in fontsize:
            size = fontsize[key]
        elif key in ['zlabel', 'ylabel', 'xlabel', 'clabel']:
            size = fontsize.get('labels', defaults.get(key, defaults.get('label')))
        elif key in ['xticks', 'yticks', 'zticks', 'cticks']:
            size = fontsize.get('ticks', defaults.get(key, defaults.get('ticks')))
        elif key in ['minor_xticks', 'minor_yticks']:
            size = fontsize.get('minor_ticks', defaults.get(key, defaults.get('minor_ticks')))
        elif key in ('legend', 'legend_title', 'title'):
            size = defaults.get(key)

        if size is None:
            return {}

        return {label: scale_fontsize(size, self.fontscale)}

    def compute_ranges(self, obj, key, ranges):
        """Given an object, a specific key, and the normalization options,
        this method will find the specified normalization options on
        the appropriate OptionTree, group the elements according to
        the selected normalization option (i.e. either per frame or
        over the whole animation) and finally compute the dimension
        ranges in each group. The new set of ranges is returned.

        """
        prev_frame = getattr(self, 'prev_frame', None)
        all_table = all(isinstance(el, Table) for el in obj.traverse(lambda x: x, [Element]))
        if obj is None or not self.normalize or all_table:
            return {}
        # Get inherited ranges
        ranges = self.ranges if ranges is None else {k: dict(v) for k, v in ranges.items()}

        # Get element identifiers from current object and resolve
        # with selected normalization options
        norm_opts = self._get_norm_opts(obj)

        # Traverse displayed object if normalization applies
        # at this level, and ranges for the group have not
        # been supplied from a composite plot
        return_fn = lambda x: x if isinstance(x, Element) else None
        for group, (axiswise, framewise, robust) in norm_opts.items():
            axiswise = (not getattr(self, 'shared_axes', True)) or (axiswise)
            elements = []
            # Skip if ranges are cached or already computed by a
            # higher-level container object.
            framewise = framewise or self.dynamic or len(elements) == 1
            if not framewise: # Traverse to get all elements
                elements = obj.traverse(return_fn, [group])
            elif key is not None: # Traverse to get elements for each frame
                frame = self._get_frame(key)
                elements = [] if frame is None else frame.traverse(return_fn, [group])

            # Only compute ranges if not axiswise on a composite plot
            # or not framewise on a Overlay or ElementPlot
            if (not (axiswise and not isinstance(obj, HoloMap)) or
                (not framewise and isinstance(obj, HoloMap))):
                self._compute_group_range(group, elements, ranges, framewise,
                                          axiswise, robust, self.top_level,
                                          prev_frame)
        self.ranges.update(ranges)
        return ranges

    def _get_norm_opts(self, obj):
        """Gets the normalization options for a LabelledData object by
        traversing the object to find elements and their ids.
        The id is then used to select the appropriate OptionsTree,
        accumulating the normalization options into a dictionary.
        Returns a dictionary of normalization options for each
        element in the tree.

        """
        norm_opts = {}

        # Get all elements' type.group.label specs and ids
        type_val_fn = lambda x: (x.id, (type(x).__name__, util.group_sanitizer(x.group, escape=False),
                                        util.label_sanitizer(x.label, escape=False))) \
            if isinstance(x, Element) else None
        element_specs = {(idspec[0], idspec[1]) for idspec in obj.traverse(type_val_fn)
                         if idspec is not None}

        # Group elements specs by ID and override normalization
        # options sequentially
        key_fn = lambda x: -1 if x[0] is None else x[0]
        id_groups = groupby(sorted(element_specs, key=key_fn), key_fn)
        for gid, element_spec_group in id_groups:
            gid = None if gid == -1 else gid
            group_specs = [el for _, el in element_spec_group]

            backend = self.renderer.backend
            optstree = Store.custom_options(
                backend=backend).get(gid, Store.options(backend=backend))
            # Get the normalization options for the current id
            # and match against customizable elements
            for opts in optstree:
                path = tuple(opts.path.split('.')[1:])
                applies = any(path == spec[:i] for spec in group_specs
                              for i in range(1, 4))
                if applies and 'norm' in opts.groups:
                    nopts = opts['norm'].options
                    popts = opts['plot'].options
                    if 'axiswise' in nopts or 'framewise' in nopts or 'clim_percentile' in popts:
                        norm_opts.update({path: (nopts.get('axiswise', False),
                                                 nopts.get('framewise', False),
                                                 popts.get('clim_percentile', False))})
        element_specs = [spec for _, spec in element_specs]
        norm_opts.update({spec: (False, False, False) for spec in element_specs
                          if not any(spec[:i] in norm_opts.keys() for i in range(1, 4))})
        return norm_opts

    @classmethod
    def _merge_group_ranges(cls, ranges):
        hard_range = util.max_range(ranges['hard'], combined=False)
        soft_range = util.max_range(ranges['soft'])
        robust_range = util.max_range(ranges.get('robust', []))
        data_range = util.max_range(ranges['data'])
        combined = util.dimension_range(data_range[0], data_range[1],
                                        hard_range, soft_range)
        dranges = {'data': data_range, 'hard': hard_range,
                   'soft': soft_range, 'combined': combined,
                   'robust': robust_range, 'values': ranges}
        if 'factors' in ranges:
            all_factors = ranges['factors']
            factor_dtypes = {fs.dtype for fs in all_factors} if all_factors else []
            dtype = next(iter(factor_dtypes)) if len(factor_dtypes) == 1 else None
            expanded = [v for fctrs in all_factors for v in fctrs]
            if dtype is not None:
                try:
                    # Try to keep the same dtype
                    expanded = np.array(expanded, dtype=dtype)
                except Exception:
                    pass
            dranges['factors'] = util.unique_array(expanded)
        return dranges

    @classmethod
    def _compute_group_range(cls, group, elements, ranges, framewise,
                             axiswise, robust, top_level, prev_frame):
        # Iterate over all elements in a normalization group
        # and accumulate their ranges into the supplied dictionary.
        elements = [el for el in elements if el is not None]

        data_ranges = {}
        robust_ranges = {}
        categorical_dims = []
        for el in elements:
            for el_dim in el.dimensions('ranges'):
                if hasattr(el, 'interface'):
                    if isinstance(el, Graph) and el_dim in el.nodes.dimensions():
                        dtype = el.nodes.interface.dtype(el.nodes, el_dim)
                    else:
                        dtype = el.interface.dtype(el, el_dim)
                elif hasattr(el, '__len__') and len(el):
                    dtype = el.dimension_values(el_dim).dtype
                else:
                    dtype = None

                if all(util.isfinite(r) for r in el_dim.range):
                    data_range = (None, None)
                elif dtype is not None and dtype_kind(dtype) in 'SU':
                    data_range = ('', '')
                elif isinstance(el, Graph) and el_dim in el.kdims[:2]:
                    data_range = el.nodes.range(2, dimension_range=False)
                elif el_dim.values:
                    ds = Dataset(el_dim.values, el_dim)
                    data_range = ds.range(el_dim, dimension_range=False)
                else:
                    data_range = el.range(el_dim, dimension_range=False)

                data_ranges[(el, el_dim)] = data_range
                if dtype is not None and dtype_kind(dtype) in 'uif' and robust:
                    percentile = 2 if isinstance(robust, bool) else robust
                    robust_ranges[(el, el_dim)] = (
                        dim(el_dim, np.nanpercentile, percentile).apply(el),
                        dim(el_dim, np.nanpercentile, 100 - percentile).apply(el)
                    )

                if (any(isinstance(r, str) for r in data_range) or
                    (el_dim.type is not None and issubclass(el_dim.type, str)) or
                    (dtype is not None and dtype_kind(dtype) in 'SU')):
                    categorical_dims.append(el_dim)

        prev_ranges = ranges.get(group, {})
        group_ranges = {}
        for el in elements:
            if isinstance(el, (Empty, Table)): continue
            opts = cls.lookup_options(el, 'style')
            plot_opts = cls.lookup_options(el, 'plot')
            opt_kwargs = dict(opts.kwargs, **plot_opts.kwargs)
            if not opt_kwargs.get('apply_ranges', True):
                continue

            # Compute normalization for color dim transforms
            for k, v in opt_kwargs.items():
                if not isinstance(v, dim) or ('color' not in k and k != 'magnitude'):
                    continue

                if isinstance(v, dim) and v.applies(el):
                    dim_name = repr(v)
                    if dim_name in prev_ranges and not framewise:
                        continue
                    values = v.apply(el, all_values=True)
                    factors = None
                    if dtype_kind(values) == 'M':
                        drange = values.min(), values.max()
                    elif util.isscalar(values):
                        drange = values, values
                    elif dtype_kind(values) in 'US':
                        factors = util.unique_array(values)
                    elif len(values) == 0:
                        drange = np.nan, np.nan
                    else:
                        try:
                            with warnings.catch_warnings():
                                warnings.filterwarnings('ignore', r'All-NaN (slice|axis) encountered')
                                drange = (np.nanmin(values), np.nanmax(values))
                        except Exception:
                            factors = util.unique_array(values)
                    if dim_name not in group_ranges:
                        group_ranges[dim_name] = {
                            'id': [], 'data': [], 'hard': [], 'soft': []
                        }

                    if factors is not None:
                        if 'factors' not in group_ranges[dim_name]:
                            group_ranges[dim_name]['factors'] = []
                        group_ranges[dim_name]['factors'].append(factors)
                    else:
                        group_ranges[dim_name]['data'].append(drange)
                    group_ranges[dim_name]['id'].append(id(el))

            # Compute dimension normalization
            for el_dim in el.dimensions('ranges'):
                dim_name = el_dim.label
                if dim_name in prev_ranges and not framewise:
                    continue
                data_range = data_ranges[(el, el_dim)]
                if dim_name not in group_ranges:
                    group_ranges[dim_name] = {
                        'id': [], 'data': [], 'hard': [], 'soft': [], 'robust': []
                    }
                group_ranges[dim_name]['data'].append(data_range)
                group_ranges[dim_name]['hard'].append(el_dim.range)
                group_ranges[dim_name]['soft'].append(el_dim.soft_range)
                if (el, el_dim) in robust_ranges:
                    group_ranges[dim_name]['robust'].append(robust_ranges[(el, el_dim)])
                if el_dim in categorical_dims:
                    if 'factors' not in group_ranges[dim_name]:
                        group_ranges[dim_name]['factors'] = []
                    if el_dim.values not in ([], None):
                        values = el_dim.values
                    elif el_dim in el:
                        if isinstance(el, Graph) and el_dim in el.kdims[:2]:
                            # Graph start/end normalization should include all node indices
                            values = el.nodes.dimension_values(2, expanded=False)
                        else:
                            values = el.dimension_values(el_dim, expanded=False)
                    elif isinstance(el, Graph) and el_dim in el.nodes:
                        values = el.nodes.dimension_values(el_dim, expanded=False)
                    if (isinstance(values, np.ndarray) and dtype_kind(values) == 'O' and
                        all(isinstance(v, (np.ndarray)) for v in values)):
                        values = np.concatenate(values) if len(values) else []
                    factors = util.unique_array(values)
                    group_ranges[dim_name]['factors'].append(factors)
                group_ranges[dim_name]['id'].append(id(el))

        # Avoid merging ranges with non-matching types
        group_dim_ranges = defaultdict(dict)
        for gdim, values in group_ranges.items():
            matching = True
            for t, rs in values.items():
                if t in ('factors', 'id'):
                    continue
                matching &= (
                    len({'date' if isinstance(v, util.datetime_types) else 'number'
                         for rng in rs for v in rng if util.isfinite(v)}) < 2
                )
            if matching:
                group_dim_ranges[gdim] = values

        # Merge ranges across elements
        dim_ranges = []
        for gdim, values in group_dim_ranges.items():
            dranges = cls._merge_group_ranges(values)
            dim_ranges.append((gdim, dranges))

        # Merge local ranges into global range dictionary
        if prev_ranges and not (top_level or axiswise) and framewise and prev_frame is not None:
            # Partially update global ranges with local changes
            prev_ids = prev_frame.traverse(lambda o: id(o))
            for d, dranges in dim_ranges:
                values = prev_ranges.get(d, {}).get('values', None)

                if values is None or 'id' not in values:
                    for g, drange in dranges.items():
                        if d not in prev_ranges:
                            prev_ranges[d] = {}
                        prev_ranges[d][g] = drange
                    continue

                ids = values.get('id')

                # Filter out ranges of updated elements and append new ranges
                merged = {}
                for g, drange in dranges['values'].items():
                    filtered = [r for i, r in zip(ids, values.get(g, []), strict=None) if i not in prev_ids]
                    filtered += drange
                    merged[g] = filtered
                prev_ranges[d] = cls._merge_group_ranges(merged)
        elif prev_ranges and not (framewise and (top_level or axiswise)):
            # Combine local with global range
            for d, dranges in dim_ranges:
                for g, drange in dranges.items():
                    prange = prev_ranges.get(d, {}).get(g, None)
                    if prange is None:
                        if d not in prev_ranges:
                            prev_ranges[d] = {}
                        prev_ranges[d][g] = drange
                    elif g in ('factors', 'values'):
                        prev_ranges[d][g] = drange
                    else:
                        prev_ranges[d][g] = util.max_range([prange, drange],
                                                           combined=g=='hard')
        else:
            # Override global range
            ranges[group] = dict(dim_ranges)

    @classmethod
    def _traverse_options(cls, obj, opt_type, opts, specs=None, keyfn=None, defaults=True):
        """Traverses the supplied object getting all options in opts for
        the specified opt_type and specs. Also takes into account the
        plotting class defaults for plot options. If a keyfn is
        supplied the returned options will be grouped by the returned
        keys.

        """
        def lookup(x):
            """Looks up options for object, including plot defaults.
            keyfn determines returned key otherwise None key is used.

            """
            options = cls.lookup_options(x, opt_type)
            selected = {o: options.options[o]
                        for o in opts if o in options.options}
            if opt_type == 'plot' and defaults:
                plot = Store.registry[cls.backend].get(type(x))
                selected['defaults'] = {o: getattr(plot, o) for o in opts
                                        if o not in selected and hasattr(plot, o)}
            key = keyfn(x) if keyfn else None
            return (key, selected)

        # Traverse object and accumulate options by key
        traversed = obj.traverse(lookup, specs)
        options = {}
        default_opts = defaultdict(lambda: defaultdict(list))
        for key, topts in traversed:
            defaults = topts.pop('defaults', {})
            if key not in options:
                options[key] = {}
            for opt, v in topts.items():
                if opt not in options[key]:
                    options[key][opt] = []
                options[key][opt].append(v)
            for opt, v in defaults.items():
                default_opts[key][opt].append(v)

        # Merge defaults into dictionary if not explicitly specified
        for key, dopts in default_opts.items():
            for opt, v in dopts.items():
                if opt not in options[key]:
                    options[key][opt] = v
        return options if keyfn else options.get(None, {})

    def _get_projection(cls, obj):
        """Uses traversal to find the appropriate projection
        for a nested object. Respects projections set on
        Overlays before considering Element based settings,
        before finally looking up the default projection on
        the plot type. If more than one non-None projection
        type is found an exception is raised.

        """
        isoverlay = lambda x: isinstance(x, CompositeOverlay)
        element3d = obj.traverse(lambda x: x, [Element3D])
        if element3d:
            return '3d'
        opts = cls._traverse_options(obj, 'plot', ['projection'],
                                     [CompositeOverlay, Element],
                                     keyfn=isoverlay)
        from_overlay = not all(p is None for p in opts.get(True, {}).get('projection', []))
        projections = opts.get(from_overlay, {}).get('projection', [])
        custom_projs = [p for p in projections if p is not None]
        if len(set(custom_projs)) > 1:
            raise ValueError("An axis may only be assigned one projection type")
        return custom_projs[0] if custom_projs else None

    def update(self, key):
        if len(self) == 1 and key in (0, self.keys[0]) and not self.drawn:
            return self.initialize_plot()
        item = self.__getitem__(key)
        self.traverse(lambda x: setattr(x, '_updated', True))
        return item

    def __len__(self):
        """Returns the total number of available frames.

        """
        return len(self.keys)


class CallbackPlot:

    backend = None

    @staticmethod
    def _sources_match(src1, src2):
        return src1 is src2 or (src1._plot_id is not None and src1._plot_id == src2._plot_id)

    def _matching_plot_type(self, element):
        """Checks if the plot type matches the element type.

        """
        return (
            (not isinstance(element, CompositeOverlay) or isinstance(self, GenericOverlayPlot) or self.batched) and
            (not isinstance(element, Element) or not isinstance(self, GenericOverlayPlot))
        )

    def _construct_callbacks(self):
        """Initializes any callbacks for streams which have defined
        the plotted object as a source.

        """
        source_streams = []
        cb_classes = set()
        registry = list(Stream.registry.items())
        callbacks = Stream._callbacks[self.backend]
        for source in self.link_sources:
            streams = []
            for stream_src, src_streams in registry:
                # Skip if source identities do not match
                if not self._sources_match(stream_src, source):
                    continue
                for stream in src_streams:
                    # Skip if Stream.source is an overlay but the plot isn't
                    # or if the source is an element but the plot isn't
                    src_el = stream.source.last if isinstance(stream.source, HoloMap) else stream.source
                    if not self._matching_plot_type(src_el):
                        continue
                    streams.append(stream)
            cb_classes |= {(callbacks[type(stream)], stream) for stream in streams
                           if type(stream) in callbacks and stream.linked
                           and stream.source is not None}
        cbs = []
        sorted_cbs = sorted(cb_classes, key=lambda x: id(x[0]))
        for cb, group in groupby(sorted_cbs, lambda x: x[0]):
            cb_streams = [s for _, s in group]
            for cb_stream in cb_streams:
                if cb_stream not in source_streams:
                    source_streams.append(cb_stream)
            cbs.append(cb(self, cb_streams, source))
        return cbs, source_streams

    @property
    def link_sources(self):
        """Returns potential Link or Stream sources.

        """
        if isinstance(self, GenericOverlayPlot):
            zorders = []
        elif self.batched:
            zorders = list(range(self.zorder, self.zorder+len(self.hmap.last)))
        else:
            zorders = [self.zorder]

        if isinstance(self, GenericOverlayPlot) and not self.batched:
            if self.overlaid:
                sources = [self.hmap.last]
            else:
                sources = [
                    o for i, inputs in self.stream_sources.items()
                    for o in inputs
                ]
        elif not self.static or isinstance(self.hmap, DynamicMap):
            sources = [o for i, inputs in self.stream_sources.items()
                       for o in inputs if i in zorders]
        else:
            sources = [self.hmap.last]
        return sources



class GenericElementPlot(DimensionedPlot):
    """Plotting baseclass to render contents of an Element. Implements
    methods to get the correct frame given a HoloMap, axis labels and
    extents and titles.

    """

    apply_ranges = param.Boolean(default=True, doc="""
        Whether to compute the plot bounds from the data itself.""")

    apply_extents = param.Boolean(default=True, doc="""
        Whether to apply extent overrides on the Elements""")

    bgcolor = param.ClassSelector(class_=(str, tuple), default=None, doc="""
        If set bgcolor overrides the background color of the axis.""")

    default_span = param.ClassSelector(default=2.0, class_=(int, float, tuple), doc="""
        Defines the span of an axis if the axis range is zero, i.e. if
        the lower and upper end of an axis are equal or no range is
        defined at all. For example if there is a single datapoint at
        0 a default_span of 2.0 will result in axis ranges spanning
        from -1 to 1.""")

    hooks = param.HookList(default=[], doc="""
        Optional list of hooks called when finalizing a plot. The
        hook is passed the plot object and the displayed element, and
        other plotting handles can be accessed via plot.handles.""")

    invert_axes = param.Boolean(default=False, doc="""
        Whether to invert the x- and y-axis""")

    invert_xaxis = param.Boolean(default=False, doc="""
        Whether to invert the plot x-axis.""")

    invert_yaxis = param.Boolean(default=False, doc="""
        Whether to invert the plot y-axis.""")

    logx = param.Boolean(default=False, doc="""
        Whether the x-axis of the plot will be a log axis.""")

    logy = param.Boolean(default=False, doc="""
        Whether the y-axis of the plot will be a log axis.""")

    padding = param.ClassSelector(default=0.1, class_=(int, float, tuple), doc="""
        Fraction by which to increase auto-ranged extents to make
        datapoints more visible around borders.

        To compute padding, the axis whose screen size is largest is
        chosen, and the range of that axis is increased by the
        specified fraction along each axis.  Other axes are then
        padded ensuring that the amount of screen space devoted to
        padding is equal for all axes. If specified as a tuple, the
        int or float values in the tuple will be used for padding in
        each axis, in order (x,y or x,y,z).

        For example, for padding=0.2 on a 800x800-pixel plot, an x-axis
        with the range [0,10] will be padded by 20% to be [-1,11], while
        a y-axis with a range [0,1000] will be padded to be [-100,1100],
        which should make the padding be approximately the same number of
        pixels. But if the same plot is changed to have a height of only
        200, the y-range will then be [-400,1400] so that the y-axis
        padding will still match that of the x-axis.

        It is also possible to declare non-equal padding value for the
        lower and upper bound of an axis by supplying nested tuples,
        e.g. padding=(0.1, (0, 0.1)) will pad the x-axis lower and
        upper bound as well as the y-axis upper bound by a fraction of
        0.1 while the y-axis lower bound is not padded at all.""")

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    show_grid = param.Boolean(default=False, doc="""
        Whether to show a Cartesian grid on the plot.""")

    xaxis = param.Selector(default='bottom',
                                 objects=['top', 'bottom', 'bare', 'top-bare',
                                          'bottom-bare', None, True, False], doc="""
        Whether and where to display the xaxis.
        The "bare" options allow suppressing all axis labels, including ticks and xlabel.
        Valid options are 'top', 'bottom', 'bare', 'top-bare' and 'bottom-bare'.""")

    yaxis = param.Selector(default='left',
                                      objects=['left', 'right', 'bare', 'left-bare',
                                               'right-bare', None, True, False], doc="""
        Whether and where to display the yaxis.
        The "bare" options allow suppressing all axis labels, including ticks and ylabel.
        Valid options are 'left', 'right', 'bare', 'left-bare' and 'right-bare'.""")

    xlabel = param.String(default=None, doc="""
        An explicit override of the x-axis label, if set takes precedence
        over the dimension label.""")

    ylabel = param.String(default=None, doc="""
        An explicit override of the y-axis label, if set takes precedence
        over the dimension label.""")

    xlim = param.Tuple(default=(np.nan, np.nan), length=2, doc="""
       User-specified x-axis range limits for the plot, as a tuple (low,high).
       If specified, takes precedence over data and dimension ranges.""")

    ylim = param.Tuple(default=(np.nan, np.nan), length=2, doc="""
       User-specified y-axis range limits for the plot, as a tuple (low,high).
       If specified, takes precedence over data and dimension ranges.""")

    zlim = param.Tuple(default=(np.nan, np.nan), length=2, doc="""
       User-specified z-axis range limits for the plot, as a tuple (low,high).
       If specified, takes precedence over data and dimension ranges.""")

    xrotation = param.Integer(default=None, bounds=(0, 360), doc="""
        Rotation angle of the xticks.""")

    yrotation = param.Integer(default=None, bounds=(0, 360), doc="""
        Rotation angle of the yticks.""")

    xticks = param.ClassSelector(class_=(int, list, tuple, np.ndarray), default=None, doc="""
        Ticks along x-axis specified as an integer, explicit list of
        tick locations. If set to None default ticking behavior is applied.""")

    yticks = param.ClassSelector(class_=(int, list, tuple, np.ndarray), default=None, doc="""
        Ticks along y-axis specified as an integer, explicit list of
        tick locations. If set to None default ticking behavior is applied.""")

    # A dictionary mapping of the plot methods used to draw the
    # glyphs corresponding to the ElementPlot, can support two
    # keyword arguments a 'single' implementation to draw an individual
    # plot and a 'batched' method to draw multiple Elements at once
    _plot_methods = {}

    # Declares the options that are propagated from sub-elements of the
    # plot, mostly useful for inheriting options from individual
    # Elements on an OverlayPlot. Enabled by default in v1.7.
    _propagate_options = []
    v17_option_propagation = True

    _deprecations = {
        'color_index': (
            "The `color_index` parameter is deprecated in favor of color "
            "style mapping, e.g. `color=dim('color')` or `line_color=dim('color')`"
        ),
        'size_index': (
            "The `size_index` parameter is deprecated in favor of size "
            "style mapping, e.g. `size=dim('size')**2`."
        ),
        'scaling_method': (
            "The `scaling_method` parameter is deprecated in favor of size "
            "style mapping, e.g. `size=dim('size')**2` for area scaling."
        ),
        'scaling_factor': (
            "The `scaling_factor` parameter is deprecated in favor of size "
            "style mapping, e.g. `size=dim('size')*10`."
        ),
        'size_fn': (
            "The `size_fn` parameter is deprecated in favor of size "
            "style mapping, e.g. `size=abs(dim('size'))`."
        ),
    }

    _selection_display = NoOpSelectionDisplay()

    _multi_y_propagation = False

    def __init__(self, element, keys=None, ranges=None, dimensions=None,
                 batched=False, overlaid=0, cyclic_index=0, zorder=0, style=None,
                 overlay_dims=None, stream_sources=None, streams=None, **params):
        if stream_sources is None:
            stream_sources = {}
        if overlay_dims is None:
            overlay_dims = {}
        self.zorder = zorder
        self.cyclic_index = cyclic_index
        self.overlaid = overlaid
        self.overlay_dims = overlay_dims

        if not isinstance(element, (HoloMap, DynamicMap)):
            self.hmap = HoloMap(initial_items=(0, element),
                               kdims=['Frame'], id=element.id)
        else:
            self.hmap = element

        if overlaid:
            self.stream_sources = stream_sources
        else:
            self.stream_sources = compute_overlayable_zorders(self.hmap)

        plot_element = self.hmap.last
        if batched and not isinstance(self, GenericOverlayPlot):
            plot_element = plot_element.last

        dynamic = isinstance(element, DynamicMap) and not element.unbounded
        self.top_level = keys is None
        if self.top_level:
            dimensions = self.hmap.kdims
            keys = list(self.hmap.data.keys())

        self.style = self.lookup_options(plot_element, 'style') if style is None else style
        plot_opts = self.lookup_options(plot_element, 'plot').options

        propagate_options = self._propagate_options.copy()
        if self._multi_y_propagation:
            propagate_options = list(set(propagate_options) - set(GenericOverlayPlot._multi_y_unpropagated))

        if self.v17_option_propagation:
            inherited = self._traverse_options(plot_element, 'plot',
                                               propagate_options,
                                               defaults=False)
            plot_opts.update(**{k: v[0] for k, v in inherited.items()
                                if k not in plot_opts})

        applied_params = dict(params, **plot_opts)
        for p, pval in applied_params.items():
            if p in self.param and p in self._deprecations and pval is not None:
                self.param.warning(self._deprecations[p])
        super().__init__(keys=keys, dimensions=dimensions,
                         dynamic=dynamic, **applied_params)
        self.batched = batched
        self.streams = get_nested_streams(self.hmap) if streams is None else streams

        # Attach streams if not overlaid and not a batched ElementPlot
        if not (self.overlaid or (self.batched and not isinstance(self, GenericOverlayPlot))):
            attach_streams(self, self.hmap)

        # Update plot and style options for batched plots
        if self.batched:
            self.ordering = util.layer_sort(self.hmap)
            overlay_opts = self.lookup_options(self.hmap.last, 'plot').options.items()
            opts = {k: v for k, v in overlay_opts if k in self.param}
            self.param.update(**opts)
            self.style = self.lookup_options(plot_element, 'style').max_cycles(len(self.ordering))
        else:
            self.ordering = []


    def get_zorder(self, overlay, key, el):
        """Computes the z-order of element in the NdOverlay
        taking into account possible batching of elements.

        """
        spec = util.get_overlay_spec(overlay, key, el)
        return self.ordering.index(spec)

    def _updated_zorders(self, overlay):
        specs = [util.get_overlay_spec(overlay, key, el)
                 for key, el in overlay.data.items()]
        self.ordering = sorted(set(self.ordering+specs))
        return [self.ordering.index(spec) for spec in specs]

    def _get_axis_dims(self, element):
        """Returns the dimensions corresponding to each axis.

        Should return a list of dimensions or list of lists of
        dimensions, which will be formatted to label the axis
        and to link axes.

        """
        dims = element.dimensions()[:2]
        if len(dims) == 1:
            return [*dims, None, None]
        else:
            return [*dims, None]

    def _has_axis_dimension(self, element, dimension):
        dims = self._get_axis_dims(element)
        return any(
            dimension in ds if isinstance(ds, list) else dimension == ds
            for ds in dims
        )

    def _get_frame(self, key):
        if isinstance(self.hmap, DynamicMap) and self.overlaid and self.current_frame:
            self.current_key = key
            return self.current_frame
        elif key == self.current_key and not self._force:
            return self.current_frame

        cached = self.current_key is None and not any(s._triggering for s in self.streams)
        key_map = dict(zip([d.name for d in self.dimensions], key, strict=None))
        frame = get_plot_frame(self.hmap, key_map, cached)
        traverse_setter(self, '_force', False)

        if key not in self.keys and len(key) == self.hmap.ndims and self.dynamic:
            self.keys.append(key)
        self.current_frame = frame
        self.current_key = key
        return frame

    def _execute_hooks(self, element):
        """Executes finalize hooks

        """
        for hook in self.hooks:
            try:
                hook(self, element)
            except Exception as e:
                self.param.warning(f"Plotting hook {hook!r} could not be "
                                   f"applied:\n\n {e}")

    def get_aspect(self, xspan, yspan):
        """Should define the aspect ratio of the plot.

        """

    def get_padding(self, obj, extents):
        """Computes padding along the axes taking into account the plot aspect.

        """
        (x0, y0, _z0, x1, y1, _z1) = extents
        padding_opt = self.lookup_options(obj, 'plot').kwargs.get('padding')
        if self.overlaid:
            padding = 0
        elif padding_opt is None:
            if self.param.objects('existing')['padding'].default is not self.padding:
                padding = self.padding
            else:
                opts = self._traverse_options(
                    obj, 'plot', ['padding'], specs=[Element], defaults=True
                )
                padding = opts.get('padding')
                if padding:
                    padding = padding[0]
                else:
                    padding = self.padding
        else:
            padding = padding_opt
        xpad, ypad, zpad = get_axis_padding(padding)
        if not self.overlaid and not self.batched:
            xspan = x1-x0 if util.is_number(x0) and util.is_number(x1) else None
            yspan = y1-y0 if util.is_number(y0) and util.is_number(y1) else None
            aspect = self.get_aspect(xspan, yspan)
            if aspect > 1:
                xpad = tuple(xp/aspect for xp in xpad) if isinstance(xpad, tuple) else xpad/aspect
            else:
                ypad = tuple(yp*aspect for yp in ypad) if isinstance(ypad, tuple) else ypad*aspect
        return xpad, ypad, zpad

    def _get_range_extents(self, element, ranges, range_type, xdim, ydim, zdim):
        dims = element.dimensions()
        ndims = len(dims)
        xdim = xdim or (dims[0] if ndims else None)
        ydim = ydim or (dims[1] if ndims > 1 else None)
        if isinstance(self.projection, str) and self.projection == '3d':
            zdim = zdim or (dims[2] if ndims > 2 else None)
        else:
            zdim = None

        (x0, x1), xsrange, xhrange = get_range(element, ranges, xdim)
        (y0, y1), ysrange, yhrange = get_range(element, ranges, ydim)
        (z0, z1), zsrange, zhrange = get_range(element, ranges, zdim)

        trigger = False
        if not self.overlaid and not self.batched:
            xspan, yspan, _zspan = (v/2. for v in get_axis_padding(self.default_span))
            mx0, mx1 = get_minimum_span(x0, x1, xspan)
            if x0 != mx0 or x1 != mx1:
                x0, x1 = mx0, mx1
                trigger = True
            my0, my1 = get_minimum_span(y0, y1, yspan)
            if y0 != my0 or y1 != my1:
                y0, y1 = my0, my1
                trigger = True

        xpad, ypad, zpad = self.get_padding(element, (x0, y0, z0, x1, y1, z1))

        if range_type == 'soft':
            x0, x1 = xsrange
        elif range_type == 'hard':
            x0, x1 = xhrange
        elif xdim == 'categorical':
            x0, x1 = '', ''
        elif range_type == 'combined':
            x0, x1 = util.dimension_range(x0, x1, xhrange, xsrange, xpad, self.logx)

        if range_type == 'soft':
            y0, y1 = ysrange
        elif range_type == 'hard':
            y0, y1 = yhrange
        elif range_type == 'combined':
            y0, y1 = util.dimension_range(y0, y1, yhrange, ysrange, ypad, self.logy)
        elif ydim == 'categorical':
            y0, y1 = '', ''
        elif ydim is None:
            y0, y1 = np.nan, np.nan

        if isinstance(self.projection, str) and self.projection == '3d':
            if range_type == 'soft':
                z0, z1 = zsrange
            elif range_type == 'data':
                z0, z1 = zhrange
            elif range_type=='combined':
                z0, z1 = util.dimension_range(z0, z1, zhrange, zsrange, zpad, self.logz)
            elif zdim == 'categorical':
                z0, z1 = '', ''
            elif zdim is None:
                z0, z1 = np.nan, np.nan
            return (x0, y0, z0, x1, y1, z1)

        if not self.drawn:
            for stream in getattr(self, 'source_streams', []):
                if (isinstance(stream, (RangeX, RangeY, RangeXY)) and
                    trigger and stream not in self._trigger):
                    self._trigger.append(stream)

        return (x0, y0, x1, y1)

    def get_extents(self, element, ranges, range_type='combined', dimension=None, xdim=None, ydim=None, zdim=None, lims_as_soft_ranges=False, **kwargs):
        """Gets the extents for the axes from the current Element. The globally
        computed ranges can optionally override the extents.

        The extents are computed by combining the data ranges, extents
        and dimension ranges. Each of these can be obtained individually
        by setting the range_type to one of:

        * 'data': Just the data ranges
        * 'extents': Element.extents
        * 'soft': Dimension.soft_range values
        * 'hard': Dimension.range values

        To obtain the combined range, which includes range padding the
        default may be used:

        * 'combined': All the range types combined and padding applied

        This allows Overlay plots to obtain each range and combine them
        appropriately for all the objects in the overlay.

        If lims_as_soft_ranges is set to True, the xlim and ylim will be treated as
        soft ranges instead of the default case as hard ranges while computing the extents.
        This is used e.g. when apply_hard_bounds is True and xlim/ylim is set, in which
        case we limit the initial viewable range to xlim/ylim, but allow navigation up to
        the abs max between the data range and xlim/ylim.

        """
        num = 6 if (isinstance(self.projection, str) and self.projection == '3d') else 4
        if self.apply_extents and range_type in ('combined', 'extents'):
            norm_opts = self.lookup_options(element, 'norm').options
            if norm_opts.get('framewise', False) or self.dynamic:
                extents = element.extents
            else:
                extent_list = self.hmap.traverse(lambda x: x.extents, [Element])
                extents = util.max_extents(
                    extent_list,
                    isinstance(self.projection, str) and self.projection == '3d'
                )
        else:
            extents = (np.nan,) * num

        if range_type == 'extents':
            return extents

        if self.apply_ranges:
            range_extents = self._get_range_extents(element, ranges, range_type, xdim, ydim, zdim)
        else:
            range_extents = (np.nan,) * num

        if getattr(self, 'shared_axes', False) and self.subplot:
            combined = util.max_extents(
                [range_extents, extents],
                isinstance(self.projection, str) and self.projection == '3d'
            )
        else:
            max_extent = []
            for l1, l2 in zip(range_extents, extents, strict=None):
                if isfinite(l2):
                    max_extent.append(l2)
                else:
                    max_extent.append(l1)
            combined = tuple(max_extent)

        if isinstance(self.projection, str) and self.projection == '3d':
            x0, y0, z0, x1, y1, z1 = combined
        else:
            x0, y0, x1, y1 = combined

        if lims_as_soft_ranges:
            # run x|ylim through max_range to ensure datetime-dtype matching with ranges
            xlim_soft_ranges = util.max_range([self.xlim])
            ylim_soft_ranges = util.max_range([self.ylim])
            x0, x1 = util.dimension_range(x0, x1, (None, None), xlim_soft_ranges)
            y0, y1 = util.dimension_range(y0, y1, (None, None), ylim_soft_ranges)
        else:
            x0, x1 = util.dimension_range(x0, x1, self.xlim, (None, None))
            y0, y1 = util.dimension_range(y0, y1, self.ylim, (None, None))

        if not self.drawn:
            x_range, y_range = ((y0, y1), (x0, x1)) if self.invert_axes else ((x0, x1), (y0, y1))
            for stream in getattr(self, 'source_streams', []):
                if isinstance(stream, RangeX):
                    params = {'x_range': x_range}
                elif isinstance(stream, RangeY):
                    params = {'y_range': y_range}
                elif isinstance(stream, RangeXY):
                    params = {'x_range': x_range, 'y_range': y_range}
                else:
                    continue
                stream.update(**params)
                if stream not in self._trigger and (self.xlim or self.ylim):
                    self._trigger.append(stream)

        if isinstance(self.projection, str) and self.projection == '3d':
            z0, z1 = util.dimension_range(z0, z1, self.zlim, (None, None))
            return (x0, y0, z0, x1, y1, z1)
        return (x0, y0, x1, y1)

    def _get_axis_labels(self, dimensions, xlabel=None, ylabel=None, zlabel=None):
        if self.xlabel is not None:
            xlabel = self.xlabel
        elif dimensions and xlabel is None:
            xdims = dimensions[0]
            xlabel = dim_axis_label(xdims) if xdims else ''

        if self.ylabel is not None:
            ylabel = self.ylabel
        elif len(dimensions) >= 2 and ylabel is None:
            ydims = dimensions[1]
            ylabel = dim_axis_label(ydims) if ydims else ''

        if getattr(self, 'zlabel', None) is not None:
            zlabel = self.zlabel
        elif (isinstance(self.projection, str) and self.projection == '3d'
              and len(dimensions) >= 3 and zlabel is None):
            zlabel = dim_axis_label(dimensions[2]) if dimensions[2] else ''
        return xlabel, ylabel, zlabel

    def _format_title_components(self, key, dimensions=True, separator='\n'):
        frame = self._get_frame(key)
        if frame is None:
            return ('', '', '', '')
        type_name = type(frame).__name__
        group = frame.group if frame.group != type_name else ''
        label = frame.label

        if self.layout_dimensions or dimensions:
            dim_title = self._frame_title(key, separator=separator)
        else:
            dim_title = ''

        return (label, group, type_name, dim_title)

    def _parse_backend_opt(self, opt, plot, model_accessor_aliases):
        """Parses a custom option of the form 'model.accessor.option'
        and returns the corresponding model and accessor.

        """
        accessors = opt.split('.')
        if len(accessors) < 2:
            self.param.warning(f"Custom option {opt!r} expects at least "
                               "two accessors separated by '.'")
            return

        model_accessor = accessors[0]

        # convert alias to handle key (figure -> fig)
        model_accessor = model_accessor_aliases.get(model_accessor) or model_accessor

        if model_accessor in self.handles:
            model = self.handles[model_accessor]
        elif hasattr(plot, model_accessor):
            model = getattr(plot, model_accessor)
        else:
            self.param.warning(
                f"{model_accessor} model could not be resolved "
                f"on {type(self).__name__!r} plot. "
                f"Ensure the {opt!r} custom option spec "
                f"references a valid model in the "
                f"plot.handles {list(self.handles.keys())!r} or on the underlying "
                f"figure object."
            )
            return

        for acc in accessors[1:-1]:
            # the logic handles resolving something like:
            # legend.get_texts()[0].set_fontsize
            if '[' in acc and acc.endswith(']'):
                getitem_index = acc.index('[')
                # gets the '0:2' or '0,2' or ':2' or '2:'
                getitem_spec = acc[getitem_index+1:-1]
                try:
                    if ':' in getitem_spec:
                        # slice notation
                        slice_parts = getitem_spec.split(':')
                        slice_start = None if slice_parts[0] == '' else int(slice_parts[0])
                        slice_stop = None if slice_parts[1] == '' else int(slice_parts[1])
                        slice_step = None if len(slice_parts) < 3 or slice_parts[2] == '' else int(slice_parts[2])
                        getitem_acc = slice(slice_start, slice_stop, slice_step)
                    elif ',' in getitem_spec:
                        # multiple items
                        getitem_acc = [literal_eval(item.strip()) for item in getitem_spec.split(',')]
                    else:
                        # single index
                        getitem_acc = literal_eval(getitem_spec)
                except Exception:
                    self.param.warning(
                        f"Could not evaluate getitem {getitem_spec!r} "
                        f"in custom option spec {opt!r}.")
                    model = None
                    break
                # gets the 'legend.get_texts()'
                acc = acc[:getitem_index]
            else:
                getitem_acc = None

            if "(" in acc and ")" in acc:
                method_ini_index = acc.index("(")
                method_end_index = acc.index(")")
                method_spec = acc[method_ini_index + 1:method_end_index]
                try:
                    if method_spec:
                        method_parts = method_spec.split(',')
                        method_args = []
                        method_kwargs = {}
                        for part in method_parts:
                            if '=' in part:
                                # Handle keyword argument
                                key, value = part.split('=')
                                method_kwargs[key.strip()] = literal_eval(value.strip())
                            else:
                                # Handle regular argument
                                method_args.append(literal_eval(part.strip()))
                    else:
                        method_args = ()
                        method_kwargs = {}
                except Exception:
                    self.param.warning(
                        f"Could not evaluate method arguments {method_spec!r} "
                        f"in custom option spec {opt!r}.")
                    model = None
                    break
                acc = acc[:method_ini_index]

                # finally, we do something with all the things we gathered above
                if not isinstance(model, list):
                    model = getattr(model, acc)(*method_args, **method_kwargs)
                else:
                    model = [getattr(m, acc)(*method_args, **method_kwargs) for m in model]

                if getitem_acc is not None:
                    if not isinstance(getitem_acc, list):
                        model = model.__getitem__(getitem_acc)
                    else:
                        model = [model.__getitem__(i) for i in getitem_acc]
                acc = acc[method_end_index:]

            if acc == "" or model is None:
                continue

            if not hasattr(model, acc):
                self.param.warning(
                    f"Could not resolve {acc!r} attribute on "
                    f"{type(model).__name__!r} model. Ensure the "
                    f"custom option spec you provided "
                    f"references a valid submodel."
                )
                model = None
                break

            model = getattr(model, acc)

        attr_accessor = accessors[-1]
        return model, attr_accessor

    def update_frame(self, key, ranges=None):
        """Set the plot(s) to the given frame number.  Operates by
        manipulating the matplotlib objects held in the self._handles
        dictionary.

        If n is greater than the number of available frames, update
        using the last available frame.

        """


class GenericOverlayPlot(GenericElementPlot):
    """Plotting baseclass to render (Nd)Overlay objects. It implements
    methods to handle the creation of ElementPlots, coordinating style
    groupings and zorder for all layers across a HoloMap. It also
    allows collapsing of layers via the Compositor.

    """

    batched = param.Boolean(default=True, doc="""
        Whether to plot Elements NdOverlay in a batched plotting call
        if possible. Disables legends and zorder may not be preserved.""")

    legend_limit = param.Integer(default=25, doc="""
        Number of rendered glyphs before legends are disabled.""")

    show_legend = param.Boolean(default=True, doc="""
        Whether to show legend for the plot.""")

    style_grouping = param.Integer(default=2, doc="""
        The length of the type.group.label spec that will be used to
        group Elements into style groups.  A style_grouping value of
        1 will group just by type, a value of 2 will group by type and
        group, and a value of 3 will group by the full specification.""")

    _passed_handles = []

    # Options not to be propagated in multi_y mode to allow independent control of y-axes
    _multi_y_unpropagated = ['yaxis', 'ylim', 'invert_yaxis', 'logy']

    def __init__(self, overlay, ranges=None, batched=True, keys=None, group_counter=None, **params):
        if 'projection' not in params:
            params['projection'] = self._get_projection(overlay)

        super().__init__(overlay, ranges=ranges, keys=keys,
                         batched=batched, **params)

        if ('multi_y' in self.param) and self.multi_y:
            for s in self.streams:
                intersection =  set(s.param) & {'y', 'y_selection', 'bounds', 'boundsy'}
                if intersection:
                    self.param.warning(f'{type(s).__name__} stream parameters'
                                       f' {list(intersection)} not yet supported with multi_y=True')

        # Apply data collapse
        self.hmap = self._apply_compositor(self.hmap, ranges, self.keys)
        self.map_lengths = Counter()
        self.group_counter = Counter() if group_counter is None else group_counter
        self.cyclic_index_lookup = {}
        self.zoffset = 0
        self.subplots = self._create_subplots(ranges)
        self.traverse(lambda x: setattr(x, 'comm', self.comm))
        self.top_level = keys is None
        self.dynamic_subplots = []
        if self.top_level:
            self.traverse(lambda x: attach_streams(self, x.hmap, 2),
                          [GenericElementPlot])

    def _apply_compositor(self, holomap, ranges=None, keys=None, dimensions=None):
        """Given a HoloMap compute the appropriate (mapwise or framewise)
        ranges in order to apply the Compositor collapse operations in
        display mode (data collapse should already have happened).

        """
        # Compute framewise normalization
        defaultdim = holomap.ndims == 1 and holomap.kdims[0].name != 'Frame'

        if keys and ranges and dimensions and not defaultdim:
            dim_inds = [dimensions.index(d) for d in holomap.kdims]
            sliced_keys = [tuple(k[i] for i in dim_inds) for k in keys]
            frame_ranges = dict([(slckey, self.compute_ranges(holomap, key, ranges[key]))
                                        for key, slckey in zip(keys, sliced_keys, strict=None) if slckey in holomap.data.keys()])
        else:
            mapwise_ranges = self.compute_ranges(holomap, None, None)
            frame_ranges = dict([(key, self.compute_ranges(holomap, key, mapwise_ranges))
                                        for key in holomap.data.keys()])
        ranges = frame_ranges.values()

        with disable_pipeline():
            collapsed = Compositor.collapse(holomap, (ranges, frame_ranges.keys()), mode='display')
        return collapsed

    @property
    def _is_batched(self):
        return self.batched and type(self.hmap.last) is NdOverlay

    def _create_subplots(self, ranges):
        # Check if plot should be batched
        ordering = util.layer_sort(self.hmap)
        batched = self._is_batched
        if batched:
            backend = self.renderer.backend
            batchedplot = Store.registry[backend].get(self.hmap.last.type)
        if (batched and batchedplot and 'batched' in batchedplot._plot_methods and
            (not self.show_legend or len(ordering) > self.legend_limit)):
            self.batched = True
            keys, vmaps = [()], [self.hmap]
        else:
            self.batched = False
            keys, vmaps = self.hmap._split_overlays()

        if isinstance(self.hmap, DynamicMap):
            dmap_streams = [streams+get_nested_streams(layer) for layer, streams in
                            zip(*split_dmap_overlay(self.hmap), strict=None)]
        else:
            dmap_streams = [None]*len(keys)

        # Compute global ordering
        length = self.style_grouping
        group_fn = lambda x: (x.type.__name__, x.last.group, x.last.label)
        for m in vmaps:
            self.map_lengths[group_fn(m)[:length]] += 1

        subplots = {}
        for (key, vmap, streams) in zip(keys, vmaps, dmap_streams, strict=None):
            if streams:
                streams = list(unique_iterator(streams))
            subplot = self._create_subplot(key, vmap, streams, ranges)
            if subplot is None:
                continue
            if not isinstance(key, tuple): key = (key,)
            subplots[key] = subplot
            if isinstance(subplot, GenericOverlayPlot):
                self.zoffset += len(subplot.subplots.keys()) - 1

        if not subplots:
            raise SkipRendering(f"{self.renderer.backend} backend could not plot any Elements "
                                "in the Overlay.")
        return subplots

    def _create_subplot(self, key, obj, streams, ranges):
        registry = Store.registry[self.renderer.backend]
        ordering = util.layer_sort(self.hmap)
        overlay_type = 1 if self.hmap.type == Overlay else 2
        group_fn = lambda x: (x.type.__name__, x.last.group, x.last.label)

        opts = {'overlaid': overlay_type}
        if self.hmap.type == Overlay:
            style_key = (obj.type.__name__, *key)
            if self.overlay_dims:
                opts['overlay_dims'] = self.overlay_dims
        else:
            if not isinstance(key, tuple): key = (key,)
            style_key = group_fn(obj) + key
            opts['overlay_dims'] = dict(zip(self.hmap.last.kdims, key, strict=None))

        if self.batched:
            vtype = type(obj.last.last)
            oidx = 0
        else:
            vtype = type(obj.last)
            if style_key not in ordering:
                ordering.append(style_key)
            oidx = ordering.index(style_key)

        plottype = registry.get(vtype, None)
        if plottype is None:
            self.param.warning(
                f"No plotting class for {vtype.__name__} type "
                f"and {self.renderer.backend} backend found. "
            )
            return None

        # Get zorder and style counter
        length = self.style_grouping
        group_key = style_key[:length]
        zorder = self.zorder + oidx + self.zoffset
        cyclic_index = self.group_counter[group_key]
        self.cyclic_index_lookup[style_key] = cyclic_index
        self.group_counter[group_key] += 1
        group_length = self.map_lengths[group_key]

        if not isinstance(plottype, PlotSelector) and issubclass(plottype, GenericOverlayPlot):
            opts['group_counter'] = self.group_counter
            opts['show_legend'] = self.show_legend
            if not any(len(frame) for frame in obj):
                self.param.warning(f'{obj.last} is empty and will be skipped '
                                   'during plotting')
                return None
        elif self.batched and 'batched' in plottype._plot_methods:
            param_vals = self.param.values()
            propagate = {opt: param_vals[opt] for opt in self._propagate_options
                         if opt in param_vals}
            opts['batched'] = self.batched
            opts['overlaid'] = self.overlaid
            opts.update(propagate)
        if len(ordering) > self.legend_limit:
            opts['show_legend'] = False
        style = self.lookup_options(obj.last, 'style').max_cycles(group_length)
        passed_handles = {k: v for k, v in self.handles.items()
                          if k in self._passed_handles}
        plotopts = dict(opts, cyclic_index=cyclic_index,
                        invert_axes=self.invert_axes,
                        dimensions=self.dimensions, keys=self.keys,
                        layout_dimensions=self.layout_dimensions,
                        ranges=ranges, show_title=self.show_title,
                        style=style, uniform=self.uniform,
                        fontsize=self.fontsize, streams=streams,
                        renderer=self.renderer, adjoined=self.adjoined,
                        stream_sources=self.stream_sources,
                        projection=self.projection, fontscale=self.fontscale,
                        zorder=zorder, root=self.root, **passed_handles)
        return plottype(obj, **plotopts)


    def _match_subplot(self, key, subplot, items, element):
        found = False
        temp_items = list(items)
        while not found:
            idx, spec, exact = dynamic_update(self, subplot, key, element, temp_items)
            if idx is not None:
                if not exact:
                    exact_matches = [
                        dynamic_update(self, subplot, k, element, temp_items)
                        for k in self.subplots
                    ]
                    exact_matches = [m for m in exact_matches if m[-1]]
                    if exact_matches:
                        idx = exact_matches[0][0]
                        temp_items.pop(idx)
                        continue
            found = True
        if idx is not None:
            idx = items.index(temp_items.pop(idx))
        return idx, spec, exact

    def _create_dynamic_subplots(self, key, items, ranges, **init_kwargs):
        """Handles the creation of new subplots when a DynamicMap returns
        a changing set of elements in an Overlay.

        """
        length = self.style_grouping
        group_fn = lambda x: (x.type.__name__, x.last.group, x.last.label)
        for k, obj in items:
            vmap = self.hmap.clone([(key, obj)])
            self.map_lengths[group_fn(vmap)[:length]] += 1
            subplot = self._create_subplot(k, vmap, [], ranges)
            if subplot is None:
                continue
            subplot.document = self.document
            if self.comm:
                subplot.comm = self.comm
            self.subplots[k] = subplot
            subplot.initialize_plot(ranges, **init_kwargs)
            subplot.update_frame(key, ranges, element=obj)
            self.dynamic_subplots.append(subplot)

    def _update_subplot(self, subplot, spec):
        """Updates existing subplots when the subplot has been assigned
        to plot an element that is not an exact match to the object
        it was initially assigned.

        """
        # See if the precise spec has already been assigned a cyclic
        # index otherwise generate a new one
        if spec in self.cyclic_index_lookup:
            cyclic_index = self.cyclic_index_lookup[spec]
        else:
            group_key = spec[:self.style_grouping]
            self.group_counter[group_key] += 1
            cyclic_index = self.group_counter[group_key]
            self.cyclic_index_lookup[spec] = cyclic_index

        subplot.cyclic_index = cyclic_index
        if subplot.overlay_dims:
            odim_key = util.wrap_tuple(spec[-1])
            new_dims = zip(subplot.overlay_dims, odim_key, strict=None)
            subplot.overlay_dims = dict(new_dims)

    def _get_subplot_extents(self, overlay, ranges, range_type, dimension=None):
        """Iterates over all subplots and collects the extents of each.

        """
        if range_type == 'combined':
            extents = {'extents': [], 'soft': [], 'hard': [], 'data': []}
        else:
            extents = {range_type: []}

        items = overlay.items()
        if self.batched and self.subplots:
            subplot = next(iter(self.subplots.values()))
            subplots = [(k, subplot) for k in overlay.data.keys()]
        else:
            subplots = self.subplots.items()

        for key, subplot in subplots:
            found = False
            if subplot is None:
                continue
            layer = overlay.data.get(key, None)
            if isinstance(self.hmap, DynamicMap) and layer is None:
                for _, layer in items:
                    if isinstance(layer, subplot.hmap.type):
                        found = True
                        break
                if not found:
                    layer = None
            if layer is None or not subplot.apply_ranges:
                continue

            if dimension and not subplot._has_axis_dimension(layer, dimension):
                continue

            if isinstance(layer, CompositeOverlay):
                sp_ranges = ranges
            else:
                sp_ranges = util.match_spec(layer, ranges) if ranges else {}
            for rt in extents:
                extent = subplot.get_extents(layer, sp_ranges, range_type=rt)
                extents[rt].append(extent)
        return extents

    def get_extents(self, overlay, ranges, range_type='combined', dimension=None, **kwargs):
        subplot_extents = self._get_subplot_extents(overlay, ranges, range_type, dimension)
        zrange = isinstance(self.projection, str) and self.projection == '3d'
        extents = {k: util.max_extents(rs, zrange) for k, rs in subplot_extents.items()}
        if range_type != 'combined':
            return extents[range_type]

        # Unpack extents
        if len(extents['data']) == 6:
            x0, y0, z0, x1, y1, z1 = extents['data']
            sx0, sy0, sz0, sx1, sy1, sz1 = extents['soft']
            hx0, hy0, hz0, hx1, hy1, hz1 = extents['hard']
        else:
            x0, y0, x1, y1 = extents['data']
            sx0, sy0, sx1, sy1 = extents['soft']
            hx0, hy0, hx1, hy1 = extents['hard']
            z0, z1 = np.nan, np.nan

        # Apply minimum span
        xspan, yspan, zspan = (v/2. for v in get_axis_padding(self.default_span))
        x0, x1 = get_minimum_span(x0, x1, xspan)
        y0, y1 = get_minimum_span(y0, y1, yspan)
        z0, z1 = get_minimum_span(z0, z1, zspan)

        # Apply padding
        xpad, ypad, zpad = self.get_padding(overlay, (x0, y0, z0, x1, y1, z1))
        x0, x1 = util.dimension_range(x0, x1, (hx0, hx1), (sx0, sx1), xpad, self.logx)
        y0, y1 = util.dimension_range(y0, y1, (hy0, hy1), (sy0, sy1), ypad, self.logy)
        if len(extents['data']) == 6:
            z0, z1 = util.dimension_range(z0, z1, (hz0, hz1), (sz0, sz1), zpad, self.logz)
            padded = (x0, y0, z0, x1, y1, z1)
        else:
            padded = (x0, y0, x1, y1)

        # Combine with Element.extents
        combined = util.max_extents([padded, extents['extents']], zrange)
        if isinstance(self.projection, str) and self.projection == '3d':
            x0, y0, z0, x1, y1, z1 = combined
        else:
            x0, y0, x1, y1 = combined

        # Apply xlim, ylim, zlim plot option
        x0, x1 = util.dimension_range(x0, x1, self.xlim, (None, None))
        if not (('multi_y' in self.param) and self.multi_y):
            y0, y1 = util.dimension_range(y0, y1, self.ylim, (None, None))

        if isinstance(self.projection, str) and self.projection == '3d':
            z0, z1 = util.dimension_range(z0, z1, getattr(self, 'zlim', (None, None)), (None, None))
            return (x0, y0, z0, x1, y1, z1)
        return (x0, y0, x1, y1)


class GenericCompositePlot(DimensionedPlot):

    def __init__(self, layout, keys=None, dimensions=None, **params):
        if 'uniform' not in params:
            params['uniform'] = traversal.uniform(layout)

        self.top_level = keys is None
        if self.top_level:
            dimensions, keys = traversal.unique_dimkeys(layout)

        dynamic, unbounded = get_dynamic_mode(layout)
        if unbounded:
            initialize_unbounded(layout, dimensions, keys[0])
        self.layout = layout
        super().__init__(keys=keys, dynamic=dynamic,
                         dimensions=dimensions, **params)
        nested_streams = layout.traverse(lambda x: get_nested_streams(x),
                                         [DynamicMap])
        self.streams = list({s for streams in nested_streams for s in streams})
        self._link_dimensioned_streams()

    def _link_dimensioned_streams(self):
        """Should perform any linking required to update titles when dimensioned
        streams change.

        """

    def _get_frame(self, key):
        """Creates a clone of the Layout with the nth-frame for each
        Element.

        """
        cached = self.current_key is None
        layout_frame = self.layout.clone(shared_data=False)
        if key == self.current_key and not self._force:
            return self.current_frame
        else:
            self.current_key = key

        key_map = dict(zip([d.name for d in self.dimensions], key, strict=None))
        for path, item in self.layout.items():
            frame = get_nested_plot_frame(item, key_map, cached)
            if frame is not None:
                layout_frame[path] = frame
        traverse_setter(self, '_force', False)

        self.current_frame = layout_frame
        return layout_frame


    def _format_title_components(self, key, dimensions=True, separator='\n'):
        dim_title = self._frame_title(key, 3, separator) if dimensions else ''
        layout = self.layout
        type_name = type(self.layout).__name__
        group = util.bytes_to_unicode(layout.group if layout.group != type_name else '')
        label = util.bytes_to_unicode(layout.label)
        return (label, group, type_name, dim_title)


class GenericLayoutPlot(GenericCompositePlot):
    """A GenericLayoutPlot accepts either a Layout or a NdLayout and
    displays the elements in a cartesian grid in scanline order.

    """

    transpose = param.Boolean(default=False, doc="""
        Whether to transpose the layout when plotting. Switches
        from row-based left-to-right and top-to-bottom scanline order
        to column-based top-to-bottom and left-to-right order.""")

    def __init__(self, layout, **params):
        if not isinstance(layout, (NdLayout, Layout)):
            raise ValueError("GenericLayoutPlot only accepts Layout objects.")
        if len(layout.values()) == 0:
            raise SkipRendering(warn=False)

        super().__init__(layout, **params)
        self.subplots = {}
        self.rows, self.cols = layout.shape[::-1] if self.transpose else layout.shape
        self.coords = list(product(range(self.rows),
                                   range(self.cols)))


class GenericAdjointLayoutPlot(Plot):
    """AdjointLayoutPlot allows placing up to three Views in a number of
    predefined and fixed layouts, which are defined by the layout_dict
    class attribute. This allows placing subviews next to a main plot
    in either a 'top' or 'right' position.

    """

    layout_dict = {'Single': {'positions': ['main']},
                   'Dual':   {'positions': ['main', 'right']},
                   'Triple': {'positions': ['main', 'right', 'top']}}
