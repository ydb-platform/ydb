from collections import namedtuple

import param

from .. import (
    Callable,
    DynamicMap,
    Element,
    GridSpace,
    HoloMap,
    Layout,
    NdOverlay,
    Overlay,
)
from ..plotting.util import initialize_dynamic
from ..streams import Derived, Stream
from . import AdjointLayout, ViewableTree
from .operation import OperationCallable

Expr = namedtuple("HoloviewsExpr", ["fn", "args", "kwargs"])
StreamIndex = namedtuple("StreamIndex", ["index"])
KDimIndex = namedtuple("KDim", ["index"])


def to_expr_extract_streams(
        hvobj, kdims, streams, original_streams, stream_mapping, container_key=None
):
    """Build a HoloViewsExpr expression tree from a potentially nested dynamic
    HoloViews object, extracting the streams and replacing them with StreamIndex
    objects.

    This function is recursive an assumes that initialize_dynamic has already
    been called on the input object.

    Parameters
    ----------
    hvobj : Element or DynamicMap or Layout
        Potentially dynamic HoloViews object to represent as a HoloviewsExpr
    kdims : list of Dimensions
        List that DynamicMap key-dimension objects should be added to
    streams : list of Stream
        List that cloned extracted streams should be added to
    original_streams : list of Stream
        List that original extracted streams should be added to
    stream_mapping : dict
        dict to be populated with mappings from container keys to extracted Stream
        objects, as described by the Callable parameter of the same name.
    container_key : int or tuple
        key into parent container that is associated to hvobj, or None if hvobj is
        not in a container

    Returns
    -------
    HoloviewsExpr expression representing hvobj if hvobj is dynamic. Otherwise,
    return hvobj itself
    """
    if isinstance(hvobj, DynamicMap):
        args = []
        kwargs = []
        dm_streams = hvobj.streams

        # Process callback inputs recursively
        input_exprs = [
            to_expr_extract_streams(
                v,
                kdims,
                streams,
                original_streams,
                stream_mapping,
                container_key=container_key,
            )
            for i, v in enumerate(hvobj.callback.inputs)
        ]
        # Record all key dimensions
        kdim_args = []
        for kdim in hvobj.kdims:
            current_kdim_names = [k.name for k in kdims]
            if kdim.name in current_kdim_names:
                # Find index to existing kdim
                idx = current_kdim_names.index(kdim.name)
                kdim_index = KDimIndex(index=idx)

                # Overwrite so that we end up with dimension object highest in the
                # object tree
                kdims[idx] = kdim
            else:
                # Add new kdim index
                kdim_index = KDimIndex(index=len(kdims))
                kdims.append(kdim)
            kdim_args.append(kdim_index)

        # Determine function
        expand_kwargs = True
        if len(input_exprs) > 1:
            fn = Overlay
            args.extend([input_exprs])
        elif isinstance(hvobj.callback, OperationCallable):
            fn = hvobj.callback.operation.instance(streams=[])
            fn.dynamic = False
            args.extend(input_exprs)

            if "kwargs" in fn.param:
                expand_kwargs = False
                if "kwargs" in hvobj.callback.operation_kwargs:
                    kwargs.append(hvobj.callback.operation_kwargs["kwargs"])
            elif hvobj.callback.operation_kwargs:
                # Preserve custom operation kwargs
                kwargs.append(hvobj.callback.operation_kwargs)
        else:
            fn = hvobj.callback.callable
            args.extend(kdim_args)

        for dm_stream in dm_streams:
            stream_arg = to_expr_extract_streams(
                dm_stream, kdims, streams,  original_streams,
                stream_mapping, container_key,
            )
            if hvobj.positional_stream_args:
                args.append(stream_arg)
            else:
                kwargs.append(stream_arg)

        if expand_kwargs:
            expr = Expr(fn, args, kwargs)
        else:
            expr = Expr(fn, args, [{"kwargs": Expr(dict, [], kwargs)}])
        return expr

    elif isinstance(hvobj, Stream):
        if isinstance(hvobj, Derived):
            stream_arg_fn = hvobj.transform_function
            stream_indexes = []
            for input_stream in hvobj.input_streams:
                stream_indexes.append(
                    to_expr_extract_streams(
                        input_stream, kdims, streams,  original_streams,
                        stream_mapping, container_key,
                    )
                )
            constants = hvobj.constants
            return Expr(
                stream_arg_fn, [stream_indexes, constants], []
            )
        else:
            # Get index for stream
            # Compute stream index
            if hvobj in original_streams:
                # Reuse index to existing stream
                stream_index = StreamIndex(index=original_streams.index(hvobj))
            else:
                # Add new stream
                stream_index = StreamIndex(index=len(streams))
                cloned_stream = hvobj.clone()
                original_streams.append(hvobj)
                streams.append(cloned_stream)
                if container_key is not None:
                    stream_mapping.setdefault(container_key, []).append(cloned_stream)
            return stream_index

    elif isinstance(hvobj, (Layout, GridSpace, NdOverlay, HoloMap, Overlay, AdjointLayout)):
        fn = hvobj.clone(data={}).clone
        args = []
        data_expr = []
        for i, (key, v) in enumerate(hvobj.data.items()):
            el = to_expr_extract_streams(
                v, kdims, streams, original_streams, stream_mapping, i
            )

            # Replace "DynamicMap" with type of the non-dynamic return element
            if isinstance(v, DynamicMap):
                initialize_dynamic(v)
                if (v.type is not None and
                        isinstance(key, tuple) and
                        isinstance(key[0], str)):
                    type_str = v.type.__name__
                    key = (key[0].replace("DynamicMap", type_str), "I")

            data_expr.append((key, el))

        if isinstance(hvobj, ViewableTree):
            # Use _process_items to ensure that keys are unique
            data_expr = ViewableTree._process_items(data_expr)

        kwargs = [{"data": data_expr}]
        return Expr(fn, args, kwargs)
    elif isinstance(hvobj, Element):
        return hvobj.clone(link=False)
    else:
        raise NotImplementedError(f"Type {type(hvobj)} not implemented")


def expr_to_fn_of_stream_contents(expr, nkdims):
    def eval_expr(expr, kdim_values, stream_values):
        if isinstance(expr, Expr):
            fn = expr.fn
            args = [eval_expr(arg, kdim_values, stream_values) for arg in expr.args]
            kwargs_list = [eval_expr(kwarg, kdim_values, stream_values) for kwarg in
                           expr.kwargs]
            kwargs = {}
            for kwargs_el in kwargs_list:
                kwargs.update(**eval_expr(kwargs_el, kdim_values, stream_values))
            # For a ParameterizedFunction (e.g. an Operation), drop keys that are not
            # accepted as params to avoid warnings
            if isinstance(fn, param.ParameterizedFunction):
                kwargs = {k: v for k, v in kwargs.items() if k in fn.param}
            return fn(*args, **kwargs)
        elif isinstance(expr, StreamIndex):
            return stream_values[expr.index]
        elif isinstance(expr, KDimIndex):
            return kdim_values[expr.index]
        elif isinstance(expr, dict):
            return {k: eval_expr(v, kdim_values, stream_values) for k, v in expr.items()}
        elif isinstance(expr, (list, tuple)):
            return type(expr)([eval_expr(v, kdim_values, stream_values) for v in expr])
        else:
            return expr

    def expr_fn(*args):
        kdim_values = args[:nkdims]
        stream_values = args[nkdims:]
        return eval_expr(expr, kdim_values, stream_values)

    return expr_fn


def decollate(hvobj):
    """Decollate transforms a potentially nested dynamic HoloViews object into single
    DynamicMap that returns a non-dynamic HoloViews object. All nested streams in the
    input object are copied and attached to the resulting DynamicMap.

    Parameters
    ----------
    hvobj : Holoviews object

    Returns
    -------
    DynamicMap
    """
    kdims = []
    original_streams = []
    streams = []
    stream_mapping = {}
    initialize_dynamic(hvobj)
    expr = to_expr_extract_streams(hvobj, kdims, streams, original_streams, stream_mapping)

    expr_fn = expr_to_fn_of_stream_contents(expr, nkdims=len(kdims))
    callback = Callable(expr_fn, stream_mapping=stream_mapping)
    return DynamicMap(
        callback, kdims=kdims, streams=streams, positional_stream_args=True
    )
