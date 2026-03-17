import operator
from types import BuiltinFunctionType, BuiltinMethodType, FunctionType, MethodType

import narwhals.stable.v2 as nw
import numpy as np
import param

from ..core.data import PandasInterface
from ..core.dimension import Dimension
from ..core.util import dtype_kind, flatten, resolve_dependent_value, unique_iterator


def _maybe_map(numpy_fn):
    def fn(values, *args, **kwargs):
        series_like = hasattr(values, 'index') and not isinstance(values, list)
        map_fn = (getattr(values, 'map_partitions', None) or
                  getattr(values, 'map_blocks', None))
        if map_fn:
            if series_like:
                return map_fn(
                    lambda s: type(s)(numpy_fn(s, *args, **kwargs),
                                      index=s.index))
            else:
                return map_fn(lambda s: numpy_fn(s, *args, **kwargs))
        elif series_like:
            return type(values)(
                numpy_fn(values, *args, **kwargs),
                index=values.index,
            )
        else:
            return numpy_fn(values, *args, **kwargs)
    return fn


def norm(values, min=None, max=None):
    """Unity-based normalization to scale data into 0-1 range.

        (values - min) / (max - min)

    Parameters
    ----------
    values
        Array of values to be normalized
    min : float, optional
        Lower bound of normalization range
    max : float, optional
        Upper bound of normalization range

    Returns
    -------
    Array of normalized values
    """
    min = np.min(values) if min is None else min
    max = np.max(values) if max is None else max
    try:
        # If it cannot be compared, this could be because of dask
        comparison = bool(min == max)
    except TypeError:
        comparison = False
    if comparison:
        return values if max == 0 else (values / max)
    return (values - min) / (max-min)


def lognorm(values, min=None, max=None):
    """Unity-based normalization on log scale.
       Apply the same transformation as matplotlib.colors.LogNorm

    Parameters
    ----------
    values
        Array of values to be normalized
    min : float, optional
        Lower bound of normalization range
    max : float, optional
        Upper bound of normalization range

    Returns
    -------
    Array of normalized values
    """
    min = np.log(np.min(values)) if min is None else np.log(min)
    max = np.log(np.max(values)) if max is None else np.log(max)
    return (np.log(values) - min) / (max-min)


class iloc:
    """Implements integer array indexing for dim expressions.

    """

    __name__ = 'iloc'

    def __init__(self, dim_expr):
        self.expr = dim_expr
        self.index = slice(None)

    def __getitem__(self, index):
        self.index = index
        return dim(self.expr, self)

    def __call__(self, values):
        import pandas as pd
        if isinstance(values, (pd.Series, pd.DataFrame)):
            return values.iloc[resolve_dependent_value(self.index)]
        else:
            return values[resolve_dependent_value(self.index)]


class loc:
    """Implements loc for dim expressions.

    """

    __name__ = 'loc'

    def __init__(self, dim_expr):
        self.expr = dim_expr
        self.index = slice(None)

    def __getitem__(self, index):
        self.index = index
        return dim(self.expr, self)

    def __call__(self, values):
        return values.loc[resolve_dependent_value(self.index)]


@_maybe_map
def bin(values, bins, labels=None):
    """Bins data into declared bins

    Bins data into declared bins. By default each bin is labelled
    with bin center values but an explicit list of bin labels may be
    defined.

    Parameters
    ----------
    values : Array of values to be binned
    bins : List or array containing the bin boundaries
    labels : List of labels to assign to each bin
        If the bins are length N the labels should be length N-1

    Returns
    -------
    Array of binned values
    """
    bins = np.asarray(bins)
    if labels is None:
        labels = (bins[:-1] + np.diff(bins)/2.)
    else:
        labels = np.asarray(labels)
    dtype = 'float' if dtype_kind(labels) == 'f' else 'O'
    binned = np.full_like(values, (np.nan if dtype == 'f' else None), dtype=dtype)
    for lower, upper, label in zip(bins[:-1], bins[1:], labels, strict=None):
        condition = (values > lower) & (values <= upper)
        binned[np.where(condition)[0]] = label
    return binned


@_maybe_map
def categorize(values, categories, default=None):
    """Maps discrete values to supplied categories.

    Replaces discrete values in input array with a fixed set of
    categories defined either as a list or dictionary.

    Parameters
    ----------
    values : Array of values to be categorized
    categories : List or dict of categories to map inputs to
    default : Default value to assign if value not in categories

    Returns
    -------
    Array of categorized values
    """
    uniq_cats = list(unique_iterator(values))
    cats = []
    for c in values:
        if isinstance(categories, list):
            cat_ind = uniq_cats.index(c)
            if cat_ind < len(categories):
                cat = categories[cat_ind]
            else:
                cat = default
        else:
            cat = categories.get(c, default)
        cats.append(cat)
    result = np.asarray(cats)
    # Convert unicode to object type like pandas does
    if dtype_kind(result) in ['U', 'S']:
        result = result.astype('object')
    return result


isin = _maybe_map(np.isin)
digitize = _maybe_map(np.digitize)
astype = _maybe_map(np.asarray)
round_ = _maybe_map(np.round)

def _python_isin(array, values):
    return [v in values for v in array]

python_isin = _maybe_map(_python_isin)

# Type of numpy function like np.max changed in Numpy 1.25
# from function to a numpy._ArrayFunctionDispatcher.
function_types = (
    BuiltinFunctionType, BuiltinMethodType, FunctionType,
    MethodType, np.ufunc, iloc, loc, type(np.max)
)


class dim:
    """dim transform objects are a way to express deferred transforms on
    Datasets. dim transforms support all mathematical and bitwise
    operators, NumPy ufuncs and methods, and provide a number of
    useful methods for normalizing, binning and categorizing data.

    """

    _binary_funcs = {
        operator.add: '+', operator.and_: '&', operator.eq: '==',
        operator.floordiv: '//', operator.ge: '>=', operator.gt: '>',
        operator.le: '<=', operator.lshift: '<<', operator.lt: '<',
        operator.mod: '%', operator.mul: '*', operator.ne: '!=',
        operator.or_: '|', operator.pow: '**', operator.rshift: '>>',
        operator.sub: '-', operator.truediv: '/'}

    _builtin_funcs = {abs: 'abs', round_: 'round'}

    _custom_funcs = {
        norm: 'norm',
        lognorm: 'lognorm',
        bin: 'bin',
        categorize: 'categorize',
        digitize: 'digitize',
        isin: 'isin',
        python_isin: 'isin',
        astype: 'astype',
        round_: 'round',
        iloc: 'iloc',
        loc: 'loc',
    }

    _numpy_funcs = {
        np.any: 'any', np.all: 'all',
        np.cumprod: 'cumprod', np.cumsum: 'cumsum', np.max: 'max',
        np.mean: 'mean', np.min: 'min',
        np.sum: 'sum', np.std: 'std', np.var: 'var', np.log: 'log',
        np.log10: 'log10'}

    _unary_funcs = {operator.pos: '+', operator.neg: '-', operator.not_: '~'}

    _all_funcs = [_binary_funcs, _builtin_funcs, _custom_funcs,
                  _numpy_funcs, _unary_funcs]

    _namespaces = {'numpy': 'np'}

    namespace = 'numpy'

    _accessor = None

    def __init__(self, obj, *args, **kwargs):
        from panel.widgets import Widget
        self.ops = []
        self._ns = np.ndarray
        self.coerce = kwargs.get('coerce', True)
        if isinstance(obj, (str, tuple)):
            self.dimension = Dimension(obj)
        elif isinstance(obj, Dimension):
            self.dimension = obj
        elif isinstance(obj, param.Parameter):
            self.dimension = obj
        elif isinstance(obj, Widget):
            self.dimension = obj.param.value
        else:
            self.dimension = obj.dimension
            self.ops = obj.ops
        if args:
            fn = args[0]
        else:
            fn = None
        if fn is not None:
            if not (isinstance(fn, (*function_types, str)) or
                    any(fn in funcs for funcs in self._all_funcs)):
                raise ValueError('Second argument must be a function, '
                                 f'found {type(fn)} type')
            self.ops = [*self.ops, {'args': args[1:], 'fn': fn, 'kwargs': kwargs, 'reverse': kwargs.pop('reverse', False)}]

    def __getstate__(self):
        return self.__dict__

    def __setstate__(self, state):
        self.__dict__.update(state)

    @property
    def _current_accessor(self):
        if self.ops and self.ops[-1]['kwargs'].get('accessor'):
            return self.ops[-1]['fn']

    def __call__(self, *args, **kwargs):
        if (not self.ops or not isinstance(self.ops[-1]['fn'], str) or
            'accessor' not in self.ops[-1]['kwargs']):
            raise ValueError(f"Cannot call method on {self!r} expression. "
                             "Only methods accessed via namespaces, "
                             "e.g. dim(...).df or dim(...).xr), "
                             "can be called.")
        op = self.ops[-1]
        if op['fn'] == 'str':
            new_op = dict(op, fn=astype, args=(str,), kwargs={})
        else:
            new_op = dict(op, args=args, kwargs=kwargs)
        return self.clone(self.dimension, [*self.ops[:-1], new_op])

    def __getattribute__(self, attr):
        self_dict = super().__getattribute__('__dict__')
        if '_ns' not in self_dict: # Not yet initialized
            return super().__getattribute__(attr)
        ns = self_dict['_ns']
        ops = super().__getattribute__('ops')
        if ops and ops[-1]['kwargs'].get('accessor'):
            try:
                ns = getattr(ns, ops[-1]['fn'])
            except Exception:
                # If the namespace doesn't know the method we are
                # calling then we are using custom API of the dim
                # transform itself, so set namespace to None
                ns = None
        extras = {ns_attr for ns_attr in dir(ns) if not ns_attr.startswith('_')}
        if attr in extras and attr not in super().__dir__():
            return type(self)(self, attr, accessor=True)
        else:
            return super().__getattribute__(attr)

    def __dir__(self):
        ns = self._ns
        if self._current_accessor:
            ns = getattr(ns, self._current_accessor)
        extras = {attr for attr in dir(ns) if not attr.startswith('_')}
        try:
            return sorted(set(super().__dir__()) | extras)
        except Exception:
            return sorted(set(dir(type(self))) | set(self.__dict__) | extras)

    def __hash__(self):
        return hash(repr(self))

    def clone(self, dimension=None, ops=None, dim_type=None):
        """Creates a clone of the dim expression optionally overriding
        the dim and ops.

        """
        dim_type = dim_type or type(self)
        if dimension is None:
            dimension = self.dimension
        new_dim = dim_type(dimension)
        if ops is None:
            ops = list(self.ops)
        new_dim.ops = ops
        return new_dim

    @classmethod
    def register(cls, key, function):
        """Register a custom dim transform function which can from then
        on be referenced by the key.

        """
        cls._custom_funcs[key] = function

    @property
    def params(self):
        from panel.widgets.base import Widget
        params = {}
        for op in self.ops:
            op_args = list(op['args'])+list(op['kwargs'].values())
            if hasattr(op['fn'], 'index'):
                # Special case for loc and iloc to check for parameters
                op_args += [op['fn'].index]
            op_args = flatten(op_args)
            for op_arg in op_args:
                if isinstance(op_arg, Widget):
                    op_arg = op_arg.param.value
                if isinstance(op_arg, dim):
                    params.update(op_arg.params)
                elif isinstance(op_arg, slice):
                    (start, stop, step) = (op_arg.start, op_arg.stop, op_arg.step)
                    if isinstance(start, Widget):
                        start = start.param.value
                    if isinstance(stop, Widget):
                        stop = stop.param.value
                    if isinstance(step, Widget):
                        step = step.param.value

                    if isinstance(start, param.Parameter):
                        params[start.name+str(id(start))] = start
                    if isinstance(stop, param.Parameter):
                        params[stop.name+str(id(stop))] = stop
                    if isinstance(step, param.Parameter):
                        params[step.name+str(id(step))] = step
                if (isinstance(op_arg, param.Parameter) and
                    isinstance(op_arg.owner, param.Parameterized)):
                    params[op_arg.name+str(id(op_arg))] = op_arg

        return params

    # Namespace properties
    @property
    def df(self):
        return self.clone(dim_type=df_dim)

    @property
    def np(self):
        return self.clone(dim_type=dim)

    @property
    def xr(self):
        return self.clone(dim_type=xr_dim)

    def __getitem__(self, *index):
        return type(self)(self, operator.getitem, *index)

    # Builtin functions
    def __abs__(self):            return type(self)(self, abs)
    def __round__(self, ndigits=None):
        args = () if ndigits is None else (ndigits,)
        return type(self)(self, round_, *args)

    # Unary operators
    def __neg__(self): return type(self)(self, operator.neg)
    def __not__(self): return type(self)(self, operator.not_)
    def __invert__(self): return type(self)(self, operator.inv)
    def __pos__(self): return type(self)(self, operator.pos)

    # Binary operators
    def __add__(self, other):       return type(self)(self, operator.add, other)
    def __and__(self, other):       return type(self)(self, operator.and_, other)
    def __div__(self, other):       return type(self)(self, operator.div, other)
    def __eq__(self, other):        return type(self)(self, operator.eq, other)
    def __floordiv__(self, other):  return type(self)(self, operator.floordiv, other)
    def __ge__(self, other):        return type(self)(self, operator.ge, other)
    def __gt__(self, other):        return type(self)(self, operator.gt, other)
    def __le__(self, other):        return type(self)(self, operator.le, other)
    def __lt__(self, other):        return type(self)(self, operator.lt, other)
    def __lshift__(self, other):    return type(self)(self, operator.lshift, other)
    def __mod__(self, other):       return type(self)(self, operator.mod, other)
    def __mul__(self, other):       return type(self)(self, operator.mul, other)
    def __ne__(self, other):        return type(self)(self, operator.ne, other)
    def __or__(self, other):        return type(self)(self, operator.or_, other)
    def __rshift__(self, other):    return type(self)(self, operator.rshift, other)
    def __pow__(self, other):       return type(self)(self, operator.pow, other)
    def __sub__(self, other):       return type(self)(self, operator.sub, other)
    def __truediv__(self, other):   return type(self)(self, operator.truediv, other)

    # Reverse binary operators
    def __radd__(self, other):      return type(self)(self, operator.add, other, reverse=True)
    def __rand__(self, other):      return type(self)(self, operator.and_, other)
    def __rdiv__(self, other):      return type(self)(self, operator.div, other, reverse=True)
    def __rfloordiv__(self, other): return type(self)(self, operator.floordiv, other, reverse=True)
    def __rlshift__(self, other):   return type(self)(self, operator.rlshift, other)
    def __rmod__(self, other):      return type(self)(self, operator.mod, other, reverse=True)
    def __rmul__(self, other):      return type(self)(self, operator.mul, other, reverse=True)
    def __ror__(self, other):       return type(self)(self, operator.or_, other, reverse=True)
    def __rpow__(self, other):      return type(self)(self, operator.pow, other, reverse=True)
    def __rrshift__(self, other):   return type(self)(self, operator.rrshift, other)
    def __rsub__(self, other):      return type(self)(self, operator.sub, other, reverse=True)
    def __rtruediv__(self, other):  return type(self)(self, operator.truediv, other, reverse=True)

    ## NumPy operations
    def __array_ufunc__(self, *args, **kwargs):
        ufunc = args[0]
        kwargs = {k: v for k, v in kwargs.items() if v is not None}
        return type(self)(self, ufunc, **kwargs)

    def clip(self, min=None, max=None):
        if min is None and max is None:
            raise ValueError('One of max or min must be given.')
        return type(self)(self, np.clip, a_min=min, a_max=max)

    def any(self, *args, **kwargs):      return type(self)(self, np.any, *args, **kwargs)
    def all(self, *args, **kwargs):      return type(self)(self, np.all, *args, **kwargs)
    def cumprod(self, *args, **kwargs):  return type(self)(self, np.cumprod,  *args, **kwargs)
    def cumsum(self, *args, **kwargs):   return type(self)(self, np.cumsum,  *args,
                                                           axis=kwargs.pop('axis',0),
                                                           **kwargs)
    def max(self, *args, **kwargs):      return type(self)(self, np.max, *args, **kwargs)
    def mean(self, *args, **kwargs):     return type(self)(self, np.mean, *args, **kwargs)
    def min(self, *args, **kwargs):      return type(self)(self, np.min, *args, **kwargs)
    def sum(self, *args, **kwargs):      return type(self)(self, np.sum, *args, **kwargs)
    def std(self, *args, **kwargs):      return type(self)(self, np.std, *args, **kwargs)
    def var(self, *args, **kwargs):      return type(self)(self, np.var, *args, **kwargs)
    def log(self, *args, **kwargs):      return type(self)(self, np.log, *args, **kwargs)
    def log10(self, *args, **kwargs):    return type(self)(self, np.log10, *args, **kwargs)

    ## Custom functions
    def astype(self, dtype): return type(self)(self, astype, dtype=dtype)
    def round(self, decimals=0): return type(self)(self, round_, decimals=decimals)
    def digitize(self, *args, **kwargs): return type(self)(self, digitize, *args, **kwargs)
    def isin(self, *args, **kwargs):
        if kwargs.pop('object', None):
            return type(self)(self, python_isin, *args, **kwargs)
        return type(self)(self, isin, *args, **kwargs)

    @property
    def iloc(self):
        return iloc(self)

    def bin(self, bins, labels=None):
        """Bins continuous values.

        Bins continuous using the provided bins and assigns labels
        either computed from each bins center point or from the
        supplied labels.

        Parameters
        ----------
        bins : List or array containing the bin boundaries

        labels : List of labels to assign to each bin
            If the bins are length N the labels should be length N-1
        """
        return type(self)(self, bin, bins, labels=labels)

    def categorize(self, categories, default=None):
        """Replaces discrete values with supplied categories

        Replaces discrete values in input array into a fixed set of
        categories defined either as a list or dictionary.

        Parameters
        ----------
        categories
            List or dict of categories to map inputs to
        default
            Default value to assign if value not in categories
        """
        return type(self)(self, categorize, categories=categories, default=default)

    def lognorm(self, limits=None):
        """Unity-based normalization log scale.
           Apply the same transformation as matplotlib.colors.LogNorm

        Parameters
        ----------
        limits
            tuple of (min, max) defining the normalization range
        """
        kwargs = {}
        if limits is not None:
            kwargs = {'min': limits[0], 'max': limits[1]}
        return type(self)(self, lognorm, **kwargs)

    def norm(self, limits=None):
        """Unity-based normalization to scale data into 0-1 range.

            (values - min) / (max - min)

        Parameters
        ----------
        limits
            tuple of (min, max) defining the normalization range
        """
        kwargs = {}
        if limits is not None:
            kwargs = {'min': limits[0], 'max': limits[1]}
        return type(self)(self, norm, **kwargs)

    @classmethod
    def pipe(cls, func, *args, **kwargs):
        """Wrapper to give multidimensional transforms a more intuitive syntax.
        For a custom function `func` with signature (*args, **kwargs), call as
        dim.pipe(func, *args, **kwargs).

        """
        args = list(args) # make mutable
        for k, arg in enumerate(args):
            if isinstance(arg, str):
                args[k] = cls(arg)
        return cls(args[0], func, *args[1:], **kwargs)

    @property
    def str(self):
        """Casts values to strings or provides str accessor.

        """
        return type(self)(self, 'str', accessor=True)

    # Other methods

    def applies(self, dataset, strict=False):
        """Determines whether the dim transform can be applied to the
        Dataset, i.e. whether all referenced dimensions can be
        resolved.

        """
        from ..element import Graph

        if isinstance(self.dimension, param.Parameter):
            applies = True
        elif isinstance(self.dimension, dim):
            applies = self.dimension.applies(dataset)
        elif self.dimension.name == '*':
            applies = True
        else:
            lookup = self.dimension if strict else self.dimension.name
            applies = dataset.get_dimension(lookup) is not None
            if isinstance(dataset, Graph) and not applies:
                applies = dataset.nodes.get_dimension(lookup) is not None

        for op in self.ops:
            args = op.get('args')
            if not args:
                continue
            for arg in args:
                if isinstance(arg, dim):
                    applies &= arg.applies(dataset)
            kwargs = op.get('kwargs')
            for kwarg in kwargs.values():
                if isinstance(kwarg, dim):
                    applies &= kwarg.applies(dataset)
        return applies

    def interface_applies(self, dataset, coerce):
        return True

    def _resolve_op(self, op, dataset, data, flat, expanded, ranges,
                    all_values, keep_index, compute, strict):
        args = op['args']
        fn = op['fn']
        kwargs = dict(op['kwargs'])
        fn_name = self._numpy_funcs.get(fn)
        if fn_name and hasattr(data, fn_name):
            if 'axis' not in kwargs and not isinstance(fn, np.ufunc):
                kwargs['axis'] = None
            fn = fn_name

        if isinstance(fn, str):
            accessor = kwargs.pop('accessor', None)
            fn_args = []
        else:
            accessor = False
            fn_args = [data]

        for arg in args:
            if isinstance(arg, dim):
                arg = arg.apply(
                    dataset, flat, expanded, ranges, all_values,
                    keep_index, compute, strict
                )
            arg = resolve_dependent_value(arg)
            fn_args.append(arg)
        fn_kwargs = {}
        for k, v in kwargs.items():
            if isinstance(v, dim):
                v = v.apply(
                    dataset, flat, expanded, ranges, all_values,
                    keep_index, compute, strict
                )
            fn_kwargs[k] = resolve_dependent_value(v)
        args = tuple(fn_args[::-1] if op['reverse'] else fn_args)
        kwargs = dict(fn_kwargs)
        return fn, fn_name, args, kwargs, accessor

    def _apply_fn(self, dataset, data, fn, fn_name, args, kwargs, accessor, drange):
        if (((fn is norm) or (fn is lognorm)) and drange != {} and
            not ('min' in kwargs and 'max' in kwargs)):
            data = fn(data, *drange)
        elif isinstance(fn, str):
            method = getattr(data, fn, None)
            if method is None:
                mtype = 'attribute' if accessor else 'method'
                raise AttributeError(
                    f"{self!r} could not be applied to '{dataset!r}', '{fn}' {mtype} "
                    f"does not exist on {type(data).__name__} type."
                )
            if accessor:
                data = method
            else:
                try:
                    data = method(*args, **kwargs)
                except Exception as e:
                    if 'axis' in kwargs:
                        kwargs.pop('axis')
                        data = method(*args, **kwargs)
                    else:
                        raise e
        else:
            data = fn(*args, **kwargs)

        return data

    def _compute_data(self, data, drop_index, compute):
        """Implements conversion of data from namespace specific object,
        e.g. pandas Series to NumPy array.

        """
        if compute and hasattr(data, 'compute'):
            data = data.compute()
        if compute and hasattr(data, 'collect'):
            data = data.collect()
        return data

    def _coerce(self, data):
        """Implements coercion of data from current data format to the
        namespace specific datatype.

        """
        return data

    def apply(self, dataset, flat=False, expanded=None, ranges=None, all_values=False,
              keep_index=False, compute=True, strict=False):
        """Evaluates the transform on the supplied dataset.

        Parameters
        ----------
        dataset
            Dataset object to evaluate the expression on
        flat
            Whether to flatten the returned array
        expanded
            Whether to use the expanded expand values
        ranges
            Dictionary for ranges for normalization
        all_values
            Whether to evaluate on all values
            Whether to evaluate on all available values, for some
            element types, such as Graphs, this may include values
            not included in the referenced column
        keep_index
            For data types that support indexes, whether the index
            should be preserved in the result.
        compute
            For data types that support lazy evaluation, whether
            the result should be computed before it is returned.
        strict
            Whether to strictly check for dimension matches
            (if False, counts any dimensions with matching names as the same)

        Returns
        -------
        values
            NumPy array computed by evaluating the expression
        """
        from ..element import Graph

        if ranges is None:
            ranges = {}

        dimension = self.dimension
        if expanded is None:
            expanded = not ((dataset.interface.gridded and dimension in dataset.kdims) or
                            (dataset.interface.multi and dataset.interface.isunique(dataset, dimension, True)))

        if not self.applies(dataset) and (not isinstance(dataset, Graph) or not self.applies(dataset.nodes)):
            raise KeyError(f"One or more dimensions in the expression {self!r} "
                           f"could not resolve on '{dataset}'. Ensure all "
                           "dimensions referenced by the expression are "
                           "present on the supplied object.")
        if not self.interface_applies(dataset, coerce=self.coerce):
            if self.coerce:
                raise ValueError(f"The expression {self!r} assumes a {self.namespace}-like "
                                 f"API but the dataset contains {dataset.interface.datatype} data "
                                 "and cannot be coerced.")
            else:
                raise ValueError(f"The expression {self!r} assumes a {self.namespace}-like "
                                 f"API but the dataset contains {dataset.interface.datatype} data "
                                 "and coercion is disabled.")

        if isinstance(dataset, Graph):
            if dimension in dataset.kdims and all_values:
                dimension = dataset.nodes.kdims[2]
            dataset = dataset if dimension in dataset else dataset.nodes

        dataset = self._coerce(dataset)
        if self.namespace != 'numpy':
            compute_for_compute = False
            keep_index_for_compute = True
        else:
            compute_for_compute = compute
            keep_index_for_compute = keep_index

        if dimension.name == '*':
            data = dataset.data
            eldim = None
        elif isinstance(dimension, param.Parameter):
            data = getattr(dimension.owner, dimension.name)
            eldim = None
        elif dataset.interface.name == "NarwhalsInterface":
            lookup = dimension if strict else dimension.name
            eldim = dataset.get_dimension(lookup).name
            if compute:
                data = dataset.interface.values(
                    dataset, lookup, expanded=expanded, flat=flat,
                    compute=compute, keep_index=keep_index,
                )
            else:
                data = nw.col(eldim)
        else:
            lookup = dimension if strict else dimension.name
            eldim = dataset.get_dimension(lookup).name
            data = dataset.interface.values(
                dataset, lookup, expanded=expanded, flat=flat,
                compute=compute_for_compute, keep_index=keep_index_for_compute
            )
        for op in self.ops:
            fn, fn_name, args, kwargs, accessor = self._resolve_op(
                op, dataset, data, flat, expanded, ranges, all_values,
                keep_index_for_compute, compute_for_compute, strict
            )
            drange = ranges.get(eldim, {})
            drange = drange.get('combined', drange)
            data = self._apply_fn(dataset, data, fn, fn_name, args,
                                  kwargs, accessor, drange)
        drop_index = keep_index_for_compute and not keep_index
        compute = not compute_for_compute and compute
        if (drop_index or compute):
            data = self._compute_data(data, drop_index, compute)
        return data

    def __repr__(self):
        op_repr = f"'{self.dimension}'"
        accessor = False
        for i, o in enumerate(self.ops):
            if i == 0:
                prev = 'dim({repr}'
            elif accessor:
                prev = '{repr}'
            else:
                prev = '({repr}'
            fn = o['fn']
            ufunc = isinstance(fn, np.ufunc)
            args = ', '.join([repr(r) for r in o['args']]) if o['args'] else ''
            kwargs = o['kwargs']
            prev_accessor = accessor
            accessor = kwargs.pop('accessor', None)
            kwargs = sorted(kwargs.items(), key=operator.itemgetter(0))
            kwargs = ', '.join(['{}={!r}'.format(*item) for item in kwargs]) if kwargs else ''
            if fn in self._binary_funcs:
                fn_name = self._binary_funcs[o['fn']]
                if o['reverse']:
                    format_string = '{args}{fn}'+prev
                else:
                    format_string = prev+'){fn}{args}'
                if any(isinstance(a, dim) for a in o['args']):
                    format_string = format_string.replace('{args}', '({args})')
            elif fn in self._unary_funcs:
                fn_name = self._unary_funcs[fn]
                format_string = '{fn}' + prev
            else:
                if isinstance(fn, str):
                    fn_name = fn
                else:
                    fn_name = fn.__name__
                if fn in self._builtin_funcs:
                    fn_name = self._builtin_funcs[fn]
                    format_string = '{fn}'+prev
                elif isinstance(fn, str):
                    if accessor:
                        sep = '' if op_repr.endswith(')') or prev_accessor else ')'
                        format_string = prev+sep+'.{fn}'
                    else:
                        format_string = prev+').{fn}('
                elif fn in self._numpy_funcs:
                    fn_name = self._numpy_funcs[fn]
                    format_string = prev+').{fn}('
                elif isinstance(fn, iloc):
                    format_string = prev+f').iloc[{fn.index!r}]'
                elif isinstance(fn, loc):
                    format_string = prev+f').loc[{fn.index!r}]'
                elif fn in self._custom_funcs:
                    fn_name = self._custom_funcs[fn]
                    format_string = prev+').{fn}('
                elif ufunc:
                    fn_name = str(fn)[8:-2]
                    if not (prev.startswith('dim') or prev.endswith(')')):
                        format_string = '{fn}' + prev
                    else:
                        format_string = '{fn}(' + prev
                    if fn_name in dir(np):
                        format_string = '.'.join([self._namespaces['numpy'], format_string])
                else:
                    format_string = prev+', {fn}'
                if accessor:
                    pass
                elif args:
                    if not format_string.endswith('('):
                        format_string += ', '
                    format_string += '{args}'
                    if kwargs:
                        format_string += ', {kwargs}'
                elif kwargs:
                    if not format_string.endswith('('):
                        format_string += ', '
                    format_string += '{kwargs}'

            # Insert accessor
            if i == 0 and self._accessor and ')' in format_string:
                idx = format_string.index(')')
                format_string = ''.join([
                    format_string[:idx], ').', self._accessor,
                    format_string[idx+1:]
                ])

            op_repr = format_string.format(fn=fn_name, repr=op_repr,
                                           args=args, kwargs=kwargs)
            if op_repr.count('(') - op_repr.count(')') > 0:
                op_repr += ')'
        if not self.ops:
            op_repr = f'dim({op_repr})'
        if op_repr.count('(') - op_repr.count(')') > 0:
            op_repr += ')'
        return op_repr


class df_dim(dim):
    """A subclass of dim which provides access to the DataFrame namespace
    along with tab-completion and type coercion allowing the expression
    to be applied on any columnar dataset.

    """

    namespace = 'dataframe'

    _accessor = 'pd'

    def __init__(self, obj, *args, **kwargs):
        import pandas as pd
        super().__init__(obj, *args, **kwargs)
        self._ns = pd.Series

    def interface_applies(self, dataset, coerce):
        return (not dataset.interface.gridded and
                (coerce or isinstance(dataset.interface, PandasInterface)))

    def _compute_data(self, data, drop_index, compute):
        if compute and hasattr(data, 'compute'):
            data = data.compute()
        if not drop_index:
            return data
        if compute and hasattr(data, 'to_numpy'):
            return data.to_numpy()
        return data.values

    def _coerce(self, dataset):
        if self.interface_applies(dataset, coerce=False):
            return dataset
        pandas_interfaces = param.concrete_descendents(PandasInterface)
        datatypes = [intfc.datatype for intfc in pandas_interfaces.values()
                     if dataset.interface.multi == intfc.multi]
        return dataset.clone(datatype=datatypes)

    @property
    def loc(self):
        return loc(self)


class xr_dim(dim):
    """A subclass of dim which provides access to the xarray DataArray
    namespace along with tab-completion and type coercion allowing
    the expression to be applied on any gridded dataset.

    """

    namespace = 'xarray'

    _accessor = 'xr'

    def __init__(self, obj, *args, **kwargs):
        try:
            import xarray as xr
        except ImportError:
            raise ImportError("XArray could not be imported, dim().xr "
                              "requires the xarray to be available.") from None
        super().__init__(obj, *args, **kwargs)
        self._ns = xr.DataArray

    def interface_applies(self, dataset, coerce):
        return (dataset.interface.gridded and
                (coerce or dataset.interface.datatype == 'xarray'))

    def _compute_data(self, data, drop_index, compute):
        if drop_index:
            data = data.data
        if hasattr(data, 'compute') and compute:
            data = data.compute()
        return data

    def _coerce(self, dataset):
        if self.interface_applies(dataset, coerce=False):
            return dataset
        return dataset.clone(datatype=['xarray'])


def lon_lat_to_easting_northing(longitude, latitude):
    """Projects the given longitude, latitude values into Web Mercator
    (aka Pseudo-Mercator or EPSG:3857) coordinates.

    Longitude and latitude can be provided as scalars, Pandas columns,
    or Numpy arrays, and will be returned in the same form.  Lists
    or tuples will be converted to Numpy arrays.

    Parameters
    ----------
    longitude

    latitude

    Returns
    -------
    (easting, northing)

    Examples
    --------
    >>> easting, northing = lon_lat_to_easting_northing(-74,40.71)

    >>> easting, northing = lon_lat_to_easting_northing(
        np.array([-74]),np.array([40.71])
    )

    >>> df=pandas.DataFrame(dict(longitude=np.array([-74]),latitude=np.array([40.71])))

    >>> df.loc[:, 'longitude'], df.loc[:, 'latitude'] = lon_lat_to_easting_northing(
        df.longitude,df.latitude
    )
    """
    if isinstance(longitude, (list, tuple)):
        longitude = np.array(longitude)
    if isinstance(latitude, (list, tuple)):
        latitude = np.array(latitude)

    origin_shift = np.pi * 6378137
    easting = longitude * origin_shift / 180.0
    with np.errstate(divide='ignore', invalid='ignore'):
        northing = np.log(
            np.tan((90 + latitude) * np.pi / 360.0)
        ) * origin_shift / np.pi
    return easting, northing


def easting_northing_to_lon_lat(easting, northing):
    """Projects the given easting, northing values into
    longitude, latitude coordinates.

    easting and northing values are assumed to be in Web Mercator
    (aka Pseudo-Mercator or EPSG:3857) coordinates.

    Parameters
    ----------
    easting

    northing

    Returns
    -------
    (longitude, latitude)
    """
    if isinstance(easting, (list, tuple)):
        easting = np.array(easting)
    if isinstance(northing, (list, tuple)):
        northing = np.array(northing)

    origin_shift = np.pi * 6378137
    longitude = easting * 180.0 / origin_shift
    with np.errstate(divide='ignore'):
        latitude = np.arctan(
            np.exp(northing * np.pi / origin_shift)
        ) * 360.0 / np.pi - 90
    return longitude, latitude
