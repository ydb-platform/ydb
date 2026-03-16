from collections import defaultdict, namedtuple

import numpy as np
import param
from param.parameterized import bothmethod

from .core.data import Dataset
from .core.element import Element, Layout
from .core.layout import AdjointLayout
from .core.options import CallbackError, Store
from .core.overlay import NdOverlay, Overlay
from .core.spaces import GridSpace
from .streams import (
    CrossFilterSet,
    Derived,
    Pipe,
    PlotReset,
    SelectionExprSequence,
    SelectMode,
    Stream,
)
from .util import DynamicMap


class _Cmap(Stream):
    cmap = param.Parameter(default=None, allow_None=True, constant=True)


class _SelectionExprOverride(Stream):
    selection_expr = param.Parameter(default=None, constant=True, doc="""
            dim expression of the current selection override""")


class _SelectionExprLayers(Derived):
    exprs = param.List(constant=True)

    def __init__(self, expr_override, cross_filter_set, **params):
        super().__init__(
            [expr_override, cross_filter_set], exclusive=True, **params
        )

    @classmethod
    def transform_function(cls, stream_values, constants):
        override_expr_values = stream_values[0]
        cross_filter_set_values = stream_values[1]

        if override_expr_values.get('selection_expr', None) is not None:
            return {"exprs": [True, override_expr_values["selection_expr"]]}
        else:
            return {"exprs": [True, cross_filter_set_values["selection_expr"]]}


_Styles = Stream.define('Styles', colors=[], alpha=1.)
_RegionElement = Stream.define("RegionElement", region_element=None)


_SelectionStreams = namedtuple(
    'SelectionStreams',
    'style_stream exprs_stream cmap_streams '
)

class _base_link_selections(param.ParameterizedFunction):
    """Baseclass for linked selection functions.

    Subclasses override the _build_selection_streams class method to construct
    a _SelectionStreams namedtuple instance that includes the required streams
    for implementing linked selections.

    Subclasses also override the _expr_stream_updated method. This allows
    subclasses to control whether new selections override prior selections or
    whether they are combined with prior selections

    """

    link_inputs = param.Boolean(default=False, doc="""
        Whether to link any streams on the input to the output.""")

    show_regions = param.Boolean(default=True, doc="""
        Whether to highlight the selected regions.""")

    @bothmethod
    def instance(self_or_cls, **params):
        inst = super().instance(**params)

        # Init private properties
        inst._cross_filter_stream = CrossFilterSet(mode=inst.cross_filter_mode)
        inst._selection_override = _SelectionExprOverride()
        inst._selection_expr_streams = {}
        inst._plot_reset_streams = {}
        inst._streams = defaultdict(list)

        # Init selection streams
        inst._selection_streams = self_or_cls._build_selection_streams(inst)

        return inst

    def _update_mode(self, event):
        if event.new == 'replace':
            self.selection_mode = 'overwrite'
        elif event.new == 'append':
            self.selection_mode = 'union'
        elif event.new == 'intersect':
            self.selection_mode = 'intersect'
        elif event.new == 'subtract':
            self.selection_mode = 'inverse'

    def _register(self, hvobj, origin=None):
        """Register an Element or DynamicMap that may be capable of generating
        selection expressions in response to user interaction events

        """
        from .element import Table

        # Create stream that produces element that displays region of selection
        selection_expr_seq = SelectionExprSequence(
            hvobj, mode=self.selection_mode,
            include_region=self.show_regions,
            index_cols=self.index_cols
        )
        self._selection_expr_streams[hvobj] = selection_expr_seq
        self._cross_filter_stream.append_input_stream(selection_expr_seq)

        self._plot_reset_streams[hvobj] = reset_stream = PlotReset(source=hvobj)

        # Register reset
        def clear_stream_history(resetting, stream=selection_expr_seq.history_stream):
            if resetting:
                stream.clear_history()
                stream.event()

        mode_stream = None
        if not isinstance(hvobj, Table):
            mode_stream = SelectMode(source=hvobj)
            mode_stream.param.watch(self._update_mode, 'mode')

        self._plot_reset_streams[hvobj].param.watch(
            clear_stream_history, ['resetting']
        )
        self._streams[origin].append((selection_expr_seq, mode_stream, reset_stream))

    def unlink(self, hvobj):
        """
        Unlinks an object which has previously been added to the
        link_selections function.

        Parameters
        ----------
        hvobj
            Component to unsubscribe from link_selections

        Examples
        --------
        >>> ls = link_selections.instance()
        >>> points1 = hv.Points(data1)
        >>> points2 = hv.Points(data2)
        >>> linked_layout = ls(points1) + ls(points2)
        >>> ls.unlink(points1)
        """
        for streams in self._streams.pop(hvobj, []):
            for stream in streams:
                if stream is None:
                    continue
                stream.source = None
                stream.clear()
                if hasattr(stream, 'cleanup'):
                    stream.cleanup()
                for obj, ses in list(self._selection_expr_streams.items()):
                    if ses is stream:
                        del self._selection_expr_streams[obj]
                for obj, prs in list(self._plot_reset_streams.items()):
                    if prs is stream:
                        del self._plot_reset_streams[obj]
                del stream

    def __call__(self, hvobj, **kwargs):
        # Apply kwargs as params
        self.param.update(**kwargs)

        if Store.current_backend not in Store.renderers:
            raise RuntimeError("Cannot perform link_selections operation "
                               f"since the selected backend {Store.current_backend!r} is not "
                               "loaded. Load the plotting extension with "
                               "hv.extension or import the plotting "
                               "backend explicitly.")

        # Perform transform
        return self._selection_transform(hvobj)

    def _selection_transform(self, hvobj, operations=(), origin=None):
        """
        Transform an input HoloViews object into a dynamic object with linked
        selections enabled.

        """
        from .plotting.util import initialize_dynamic
        if isinstance(hvobj, DynamicMap):
            callback = hvobj.callback
            if len(callback.inputs) > 1:
                return Overlay([
                    self._selection_transform(el, operations, hvobj if origin is None else origin)
                    for el in callback.inputs
                ]).collate()

            initialize_dynamic(hvobj)
            if issubclass(hvobj.type, Element):
                self._register(hvobj, hvobj if origin is None else origin)
                chart = Store.registry[Store.current_backend][hvobj.type]
                return chart.selection_display(hvobj).build_selection(
                    self._selection_streams, hvobj, operations,
                    self._selection_expr_streams.get(hvobj, None), cache=self._cache
                )
            elif (issubclass(hvobj.type, Overlay) and
                  getattr(hvobj.callback, "name", None) == "dynamic_mul"):
                return Overlay([
                    self._selection_transform(el, operations=operations)
                    for el in callback.inputs
                ]).collate()
            elif getattr(hvobj.callback, "name", None) == "dynamic_operation":
                obj = callback.inputs[0]
                return self._selection_transform(obj, operations=operations).apply(
                    callback.operation)
            else:
                # This is a DynamicMap that we don't know how to recurse into.
                self.param.warning(
                    "linked selection: Encountered DynamicMap that we don't know "
                    f"how to recurse into:\n{hvobj!r}"
                )
                return hvobj
        elif isinstance(hvobj, Element):
            # Register hvobj to receive selection expression callbacks
            chart = Store.registry[Store.current_backend][type(hvobj)]
            if getattr(chart, 'selection_display', None):
                element = hvobj.clone(link=self.link_inputs)
                self._register(element, hvobj if origin is None else origin)
                return chart.selection_display(element).build_selection(
                    self._selection_streams, element, operations,
                    self._selection_expr_streams.get(element, None), cache=self._cache
                )
            return hvobj
        elif isinstance(hvobj, (Layout, Overlay, NdOverlay, GridSpace, AdjointLayout)):
            data = dict([(k, self._selection_transform(v, operations, origin))
                         for k, v in hvobj.items()])
            if isinstance(hvobj, NdOverlay):
                def compose(*args, **kwargs):
                    new = []
                    for k, v in data.items():
                        for i, el in enumerate(v[()]):
                            if i == len(new):
                                new.append([])
                            new[i].append((k, el))
                    return Overlay([hvobj.clone(n) for n in new])
                new_hvobj = DynamicMap(compose)
                new_hvobj.callback.inputs[:] = list(data.values())
            else:
                new_hvobj = hvobj.clone(data)
                if hasattr(new_hvobj, 'collate'):
                    new_hvobj = new_hvobj.collate()
            return new_hvobj
        else:
             # Unsupported object
            return hvobj

    @classmethod
    def _build_selection_streams(cls, inst):
        """Subclasses should override this method to return a _SelectionStreams
        instance

        """
        raise NotImplementedError()

    def _expr_stream_updated(self, hvobj, selection_expr, bbox, region_element, **kwargs):
        """Called when one of the registered HoloViews objects produces a new
        selection expression.  Subclasses should override this method, and
        they should use the input expression to update the `exprs_stream`
        property of the _SelectionStreams instance that was produced by
        the _build_selection_streams.

        Subclasses have the flexibility to control whether the new selection
        express overrides previous selections, or whether it is combined with
        previous selections.

        """
        raise NotImplementedError()


class link_selections(_base_link_selections):
    """Operation which automatically links selections between elements
    in the supplied HoloViews object. Can be used a single time or
    be used as an instance to apply the linked selections across
    multiple objects.

    """

    cross_filter_mode = param.Selector(
        objects=['overwrite', 'intersect'], default='intersect', doc="""
        Determines how to combine selections across different
        elements.""")

    index_cols = param.List(default=None, doc="""
        If provided, selection switches to index mode where all queries
        are expressed solely in terms of discrete values along the
        index_cols.  All Elements given to link_selections must define the index_cols, either as explicit dimensions or by sharing an underlying Dataset that defines them.""")

    selection_expr = param.Parameter(default=None, doc="""
        dim expression of the current selection or None to indicate
        that everything is selected.""")

    selected_color = param.Color(default=None, allow_None=True, doc="""
        Color of selected data, or None to use the original color of
        each element.""")

    selection_mode = param.Selector(
        objects=['overwrite', 'intersect', 'union', 'inverse'], default='overwrite', doc="""
        Determines how to combine successive selections on the same
        element.""")

    unselected_alpha = param.Magnitude(default=0.1, doc="""
        Alpha of unselected data.""")

    unselected_color = param.Color(default=None, doc="""
        Color of unselected data.""")

    @bothmethod
    def instance(self_or_cls, **params):
        inst = super().instance(**params)

        # Initialize private properties
        inst._obj_selections = {}
        inst._obj_regions = {}
        inst._reset_regions = True
        inst._user_show_regions = inst.show_regions
        inst._updating_show_regions_internal = False

        # _datasets caches
        inst._datasets = []
        inst._cache = {}

        self_or_cls._install_param_callbacks(inst)

        return inst

    @param.depends('show_regions', watch=True)
    def _update_user_show_regions(self):
        if self._updating_show_regions_internal:
            return
        self._user_show_regions = self.show_regions

    @param.depends('selection_expr', watch=True)
    def _update_pipes(self):
        sel_expr = self.selection_expr
        for pipe, ds, raw in self._datasets:
            ref = ds._plot_id
            self._cache[ref] = ds_cache = self._cache.get(ref, {})
            if sel_expr in ds_cache:
                data = ds_cache[sel_expr]
                return pipe.event(data=data.data)
            else:
                ds_cache.clear()
            sel_ds = SelectionDisplay._select(ds, sel_expr, self._cache)
            ds_cache[sel_expr] = sel_ds
            pipe.event(data=sel_ds.data if raw else sel_ds)

    def selection_param(self, data):
        """Returns a parameter which reflects the current selection
        when applied to the supplied data, making it easy to create
        a callback which depends on the current selection.

        Parameters
        ----------
        data
            A Dataset type or data which can be cast to a Dataset

        Returns
        -------
        A parameter which reflects the current selection
        """
        raw = False
        if not isinstance(data, Dataset):
            raw = True
            data = Dataset(data)
        pipe = Pipe()
        self._datasets.append((pipe, data, raw))
        self._update_pipes()
        return pipe.param.data

    def filter(self, data, selection_expr=None):
        """Filters the provided data based on the current state of the
        current selection expression.

        Parameters
        ----------
        data
            A Dataset type or data which can be cast to a Dataset
        selection_expr
            Optionally provide your own selection expression

        Returns
        -------
        The filtered data
        """
        expr = self.selection_expr if selection_expr is None else selection_expr
        if expr is None:
            return data
        is_dataset = isinstance(data, Dataset)
        if not is_dataset:
            data = Dataset(data)
        filtered = data[expr.apply(data)]
        return filtered if is_dataset else filtered.data

    @bothmethod
    def _install_param_callbacks(self_or_cls, inst):
        def update_selection_mode(*_):
            # Reset selection state of streams
            for stream in inst._selection_expr_streams.values():
                stream.reset()
                stream.mode = inst.selection_mode

        inst.param.watch(
            update_selection_mode, ['selection_mode']
        )

        def update_cross_filter_mode(*_):
            inst._cross_filter_stream.reset()
            inst._cross_filter_stream.mode = inst.cross_filter_mode

        inst.param.watch(
            update_cross_filter_mode, ['cross_filter_mode']
        )

        def update_show_region(*_):
            for stream in inst._selection_expr_streams.values():
                stream.include_region = inst.show_regions
                stream.event()

        inst.param.watch(
            update_show_region, ['show_regions']
        )

        def update_selection_expr(*_):
            new_selection_expr = inst.selection_expr
            current_selection_expr = inst._cross_filter_stream.selection_expr
            if repr(new_selection_expr) != repr(current_selection_expr):
                # Reset the streams
                for s in inst._selection_expr_streams.values():
                    s.reset()
                    s.event()
                # Disable regions if setting selection_expr directly
                if inst._user_show_regions:
                    inst._updating_show_regions_internal = True
                    inst.show_regions = False
                    inst._updating_show_regions_internal = False
                inst._selection_override.event(selection_expr=new_selection_expr)
                inst._cross_filter_stream.selection_expr = new_selection_expr

        inst.param.watch(
            update_selection_expr, ['selection_expr']
        )

        def selection_expr_changed(*_):
            new_selection_expr = inst._cross_filter_stream.selection_expr
            if repr(inst.selection_expr) != repr(new_selection_expr):
                inst.selection_expr = new_selection_expr
                if inst._user_show_regions:
                    inst._updating_show_regions_internal = True
                    inst.show_regions = True
                    inst._updating_show_regions_internal = False

        inst._cross_filter_stream.param.watch(
            selection_expr_changed, ['selection_expr']
        )

        # Clear selection expr sequence history on plot reset
        for stream in inst._selection_expr_streams.values():
            def clear_stream_history(resetting, stream=stream):
                if resetting:
                    stream.clear_history()
            stream.plot_reset_stream.param.watch(
                clear_stream_history, ['resetting']
            )


    @classmethod
    def _build_selection_streams(cls, inst):
        # Colors stream
        style_stream = _Styles(
            colors=[inst.unselected_color, inst.selected_color],
            alpha=inst.unselected_alpha
        )

        # Cmap streams
        cmap_streams = [
            _Cmap(cmap=inst.unselected_cmap),
            _Cmap(cmap=inst.selected_cmap),
        ]

        def update_colors(*_):
            colors = [inst.unselected_color, inst.selected_color]
            style_stream.event(colors=colors, alpha=inst.unselected_alpha)
            cmap_streams[0].event(cmap=inst.unselected_cmap)
            if cmap_streams[1] is not None:
                cmap_streams[1].event(cmap=inst.selected_cmap)

        inst.param.watch(update_colors,['unselected_color', 'selected_color', 'unselected_alpha'])

        # Exprs stream
        exprs_stream = _SelectionExprLayers(
            inst._selection_override, inst._cross_filter_stream
        )

        return _SelectionStreams(
            style_stream=style_stream,
            exprs_stream=exprs_stream,
            cmap_streams=cmap_streams,
        )

    @property
    def unselected_cmap(self):
        """The datashader colormap for unselected data

        """
        if self.unselected_color is None:
            return None
        return _color_to_cmap(self.unselected_color)

    @property
    def selected_cmap(self):
        """The datashader colormap for selected data

        """
        return None if self.selected_color is None else _color_to_cmap(self.selected_color)


class SelectionDisplay:
    """Base class for selection display classes.  Selection display classes are
    responsible for transforming an element (or DynamicMap that produces an
    element) into a HoloViews object that represents the current selection
    state.

    """

    def __call__(self, element):
        return self

    def build_selection(self, selection_streams, hvobj, operations, region_stream=None, cache=None):
        if cache is None:
            cache = {}
        raise NotImplementedError()

    @staticmethod
    def _select(element, selection_expr, cache=None):
        if cache is None:
            cache = {}
        from .element import Curve, Spread
        from .util.transform import dim
        if isinstance(selection_expr, dim):
            dataset = element.dataset
            mask = None
            if dataset._plot_id in cache:
                ds_cache = cache[dataset._plot_id]
                if selection_expr in ds_cache:
                    mask = ds_cache[selection_expr]
                else:
                    ds_cache.clear()
            else:
                ds_cache = cache[dataset._plot_id] = {}
            try:
                if dataset.interface.gridded:
                    if mask is None:
                        mask = selection_expr.apply(dataset, expanded=True, flat=False, strict=False)
                    selection = dataset.clone(dataset.interface.mask(dataset, ~mask))
                elif dataset.interface.multi:
                    if mask is None:
                        mask = selection_expr.apply(dataset, expanded=False, flat=False, strict=False)
                    selection = dataset.iloc[mask]
                elif isinstance(element, (Curve, Spread)) and hasattr(dataset.interface, 'mask'):
                    if mask is None:
                        mask = selection_expr.apply(dataset, keep_index=True, strict=False)
                    selection = dataset.clone(dataset.interface.mask(dataset, ~mask))
                else:
                    if mask is None:
                        mask = selection_expr.apply(dataset, compute=False, keep_index=True, strict=False)
                    selection = dataset.select(selection_mask=mask)
            except KeyError as e:
                key_error = str(e).replace('"', '').replace('.', '')
                raise CallbackError("linked_selection aborted because it could not "
                                    f"display selection for all elements: {key_error} on '{element!r}'.") from e
            except Exception as e:
                raise CallbackError("linked_selection aborted because it could not "
                                    f"display selection for all elements: {e}.") from e
            ds_cache[selection_expr] = mask
        else:
            selection = element
        return selection



class NoOpSelectionDisplay(SelectionDisplay):
    """Selection display class that returns input element unchanged. For use with
    elements that don't support displaying selections.

    """

    def build_selection(self, selection_streams, hvobj, operations, region_stream=None, cache=None):
        return hvobj


class OverlaySelectionDisplay(SelectionDisplay):
    """Selection display base class that represents selections by overlaying
    colored subsets on top of the original element in an Overlay container.

    """

    def __init__(self, color_prop='color', is_cmap=False, supports_region=True):
        if not isinstance(color_prop, (list, tuple)):
            self.color_props = [color_prop]
        else:
            self.color_props = color_prop
        self.is_cmap = is_cmap
        self.supports_region = supports_region

    def _get_color_kwarg(self, color):
        return {color_prop: [color] if self.is_cmap else color
                for color_prop in self.color_props}

    def build_selection(self, selection_streams, hvobj, operations, region_stream=None, cache=None):
        from .element import Histogram

        num_layers = len(selection_streams.style_stream.colors)
        if not num_layers:
            return Overlay()

        layers = []
        for layer_number in range(num_layers):
            streams = [selection_streams.exprs_stream]
            obj = hvobj.clone(link=False) if layer_number == 1 else hvobj
            cmap_stream = selection_streams.cmap_streams[layer_number]
            layer = obj.apply(
                self._build_layer_callback, streams=[cmap_stream, *streams],
                layer_number=layer_number, cache=cache, per_element=True
            )
            layers.append(layer)

        for layer_number in range(num_layers):
            layer = layers[layer_number]
            cmap_stream = selection_streams.cmap_streams[layer_number]
            streams = [selection_streams.style_stream, cmap_stream]
            layer = layer.apply(
                self._apply_style_callback, layer_number=layer_number,
                streams=streams, per_element=True
            )
            layers[layer_number] = layer

        # Build region layer
        if region_stream is not None and self.supports_region:
            def update_region(element, region_element, colors, **kwargs):
                unselected_color = colors[0]
                if region_element is None:
                    region_element = element._empty_region()
                return self._style_region_element(region_element, unselected_color)

            streams = [region_stream, selection_streams.style_stream]
            region = hvobj.clone(link=False).apply(update_region, streams, link_dataset=False)

            eltype = hvobj.type if isinstance(hvobj, DynamicMap) else type(hvobj)
            if getattr(eltype, '_selection_dims', None) == 1 or issubclass(eltype, Histogram):
                layers.insert(1, region)
            else:
                layers.append(region)
        return Overlay(layers).collate()

    @classmethod
    def _inject_cmap_in_pipeline(cls, pipeline, cmap):
        operations = []
        for op in pipeline.operations:
            if hasattr(op, 'cmap'):
                op = op.instance(cmap=cmap)
            operations.append(op)
        return pipeline.instance(operations=operations)

    def _build_layer_callback(self, element, exprs, layer_number, cmap, cache, **kwargs):
        selection = self._select(element, exprs[layer_number], cache)
        pipeline = element.pipeline
        if cmap is not None:
            pipeline = self._inject_cmap_in_pipeline(pipeline, cmap)
        if element is selection:
            return pipeline(element.dataset)
        else:
            return pipeline(selection)

    def _apply_style_callback(self, element, layer_number, colors, cmap, alpha, **kwargs):
        opts = {}
        if layer_number == 0:
            opts['colorbar'] = False
        else:
            alpha = 1
        if cmap is not None:
            opts['cmap'] = cmap
        color = colors[layer_number] if colors else None
        return self._build_element_layer(element, color, alpha, **opts)

    def _build_element_layer(self, element, layer_color, layer_alpha, selection_expr=True):
        raise NotImplementedError()

    def _style_region_element(self, region_element, unselected_cmap):
        raise NotImplementedError()


class ColorListSelectionDisplay(SelectionDisplay):
    """Selection display class for elements that support coloring by a
    vectorized color list.

    """

    def __init__(self, color_prop='color', alpha_prop='alpha', backend=None):
        self.color_props = [color_prop]
        self.alpha_props = [alpha_prop]
        self.backend = backend

    def build_selection(self, selection_streams, hvobj, operations, region_stream=None, cache=None):
        if cache is None:
            cache = {}
        def _build_selection(el, colors, alpha, exprs, **kwargs):
            from .plotting.util import linear_gradient
            ds = el.dataset
            selection_exprs = exprs[1:]
            unselected_color = colors[0]

            # Use darker version of unselected_color if not selected color provided
            unselected_color = unselected_color or "#e6e9ec"
            backup_clr = linear_gradient(unselected_color, "#000000", 7)[2]
            selected_colors = [c or backup_clr for c in colors[1:]]
            n = len(ds)
            clrs = np.array([unselected_color, *selected_colors])

            color_inds = np.zeros(n, dtype='int8')

            for i, expr in zip(range(1, len(clrs)), selection_exprs, strict=None):
                if not expr:
                    color_inds[:] = i
                else:
                    color_inds[expr.apply(ds)] = i

            colors = clrs[color_inds]
            color_opts = {color_prop: colors for color_prop in self.color_props}
            return el.pipeline(ds).opts(backend=self.backend, clone=True, **color_opts)

        sel_streams = [selection_streams.style_stream, selection_streams.exprs_stream]
        hvobj = hvobj.apply(_build_selection, streams=sel_streams, per_element=True, cache=cache)

        for op in operations:
            hvobj = op(hvobj)

        return hvobj


def _color_to_cmap(color):
    """Create a light to dark cmap list from a base color

    """
    from .plotting.util import linear_gradient
    # Lighten start color by interpolating toward white
    start_color = linear_gradient("#ffffff", color, 7)[2]

    # Darken end color by interpolating toward black
    end_color = linear_gradient(color, "#000000", 7)[2]
    return linear_gradient(start_color, end_color, 64)
