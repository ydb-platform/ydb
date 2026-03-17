import numpy as np
import param

from ..core import Dataset, Dimension, Element2D, NdOverlay, Overlay, util
from ..core.dimension import process_dimensions
from .geom import (  # noqa: F401 backward compatible import
    Points,
    Rectangles,
    VectorField,
)
from .selection import Selection1DExpr


class Chart(Dataset, Element2D):
    """A Chart is an abstract baseclass for elements representing one or
    more independent and dependent variables defining a 1D coordinate
    system with associated values. The independent variables or key
    dimensions map onto the x-axis while the dependent variables are
    usually mapped to the location, height or spread along the
    y-axis. Any number of additional value dimensions may be
    associated with a Chart.

    If a chart's independent variable (or key dimension) is numeric
    the chart will represent a discretely sampled version of the
    underlying continuously sampled 1D space. Therefore indexing along
    this variable will automatically snap to the closest coordinate.

    Since a Chart is a subclass of a Dataset it supports the full set
    of data interfaces but usually each dimension of a chart represents
    a column stored in a dictionary, array or DataFrame.

    """

    kdims = param.List(default=[Dimension('x')], bounds=(1,2), doc="""
        The key dimension(s) of a Chart represent the independent
        variable(s).""")

    group = param.String(default='Chart', constant=True)

    vdims = param.List(default=[Dimension('y')], bounds=(1, None), doc="""
        The value dimensions of the Chart, usually corresponding to a
        number of dependent variables.""")

    # Enables adding index if 1D array like data is supplied
    _auto_indexable_1d = True

    _max_kdim_count = 1 # Remove once kdims has bounds=(1,1) instead of warning
    __abstract = True

    def __init__(self, data, kdims=None, vdims=None, **params):
        params.update(process_dimensions(kdims, vdims))
        if len(params.get('kdims', [])) == self._max_kdim_count + 1:
            self.param.warning('Chart elements should only be supplied a single kdim')
        super().__init__(data, **params)

    def __getitem__(self, index):
        return super().__getitem__(index)


class Scatter(Selection1DExpr, Chart):
    """Scatter is a Chart element representing a set of points in a 1D
    coordinate system where the key dimension maps to the points
    location along the x-axis while the first value dimension
    represents the location of the point along the y-axis.

    """

    group = param.String(default='Scatter', constant=True)


class Curve(Selection1DExpr, Chart):
    """Curve is a Chart element representing a line in a 1D coordinate
    system where the key dimension maps on the line x-coordinate and
    the first value dimension represents the height of the line along
    the y-axis.

    """

    group = param.String(default='Curve', constant=True)


class ErrorBars(Selection1DExpr, Chart):
    """ErrorBars is a Chart element representing error bars in a 1D
    coordinate system where the key dimension corresponds to the
    location along the x-axis and the first value dimension
    corresponds to the location along the y-axis and one or two
    extra value dimensions corresponding to the symmetric or
    asymmetric errors either along x-axis or y-axis. If two value
    dimensions are given, then the last value dimension will be
    taken as symmetric errors. If three value dimensions are given
    then the last two value dimensions will be taken as negative and
    positive errors. By default the errors are defined along y-axis.
    A parameter `horizontal`, when set `True`, will define the errors
    along the x-axis.

    """

    group = param.String(default='ErrorBars', constant=True, doc="""
        A string describing the quantity measured by the ErrorBars
        object.""")

    vdims = param.List(default=[Dimension('y'), Dimension('yerror')],
                       bounds=(1, None), constant=True)

    horizontal = param.Boolean(default=False, doc="""
        Whether the errors are along y-axis (vertical) or x-axis.""")

    def range(self, dim, data_range=True, dimension_range=True):
        """Return the lower and upper bounds of values along dimension.

        Range of the y-dimension includes the symmetric or asymmetric
        error.

        Parameters
        ----------
        dimension
            The dimension to compute the range on.
        data_range : bool
            Compute range from data values
        dimension_range : bool
            Include Dimension ranges
            Whether to include Dimension range and soft_range
            in range calculation

        Returns
        -------
        Tuple containing the lower and upper bound
        """
        dim_with_err = 0 if self.horizontal else 1
        didx = self.get_dimension_index(dim)
        dim = self.get_dimension(dim)
        if didx == dim_with_err and data_range and len(self):
            mean = self.dimension_values(didx)
            neg_error = self.dimension_values(2)
            if len(self.dimensions()) > 3:
                pos_error = self.dimension_values(3)
            else:
                pos_error = neg_error
            lower = np.nanmin(mean-neg_error)
            upper = np.nanmax(mean+pos_error)
            if not dimension_range:
                return (lower, upper)
            return util.dimension_range(lower, upper, dim.range, dim.soft_range)
        return super().range(dim, data_range)


class Spread(ErrorBars):
    """Spread is a Chart element representing a spread of values or
    confidence band in a 1D coordinate system. The key dimension(s)
    corresponds to the location along the x-axis and the value
    dimensions define the location along the y-axis as well as the
    symmetric or asymmetric spread.

    """

    group = param.String(default='Spread', constant=True)


class Bars(Selection1DExpr, Chart):
    """Bars is a Chart element representing categorical observations
    using the height of rectangular bars. The key dimensions represent
    the categorical groupings of the data, but may also be used to
    stack the bars, while the first value dimension represents the
    height of each bar.

    """

    group = param.String(default='Bars', constant=True)

    kdims = param.List(default=[Dimension('x')], bounds=(1,3))

    _max_kdim_count = 3


class Histogram(Selection1DExpr, Chart):
    """Histogram is a Chart element representing a number of bins in a 1D
    coordinate system. The key dimension represents the binned values,
    which may be declared as bin edges or bin centers, while the value
    dimensions usually defines a count, frequency or density associated
    with each bin.

    """

    datatype = param.List(default=['grid'])

    group = param.String(default='Histogram', constant=True)

    kdims = param.List(default=[Dimension('x')], bounds=(1,1), doc="""
        Dimensions on Element2Ds determine the number of indexable
        dimensions.""")

    vdims = param.List(default=[Dimension('Frequency')], bounds=(1, None))

    _binned = True

    def __init__(self, data, **params):
        if data is None:
            data = []
        if (isinstance(data, tuple) and len(data) == 2 and
            len(data[0])+1 == len(data[1])):
            data = data[::-1]

        super().__init__(data, **params)

    @property
    def edges(self):
        """Property to access the Histogram edges provided for backward compatibility

        """
        return self.interface.coords(self, self.kdims[0], edges=True)


class Spikes(Selection1DExpr, Chart):
    """Spikes is a Chart element which represents a number of discrete
    spikes, events or observations in a 1D coordinate system. The key
    dimension therefore represents the position of each spike along
    the x-axis while the first value dimension, if defined, controls
    the height along the y-axis. It may therefore be used to visualize
    the distribution of discrete events, representing a rug plot, or
    to draw the strength some signal.

    """

    group = param.String(default='Spikes', constant=True)

    kdims = param.List(default=[Dimension('x')], bounds=(1, 1))

    vdims = param.List(default=[], bounds=(0, None))

    _auto_indexable_1d = False


class Area(Curve):
    """Area is a Chart element representing the area under a curve or
    between two curves in a 1D coordinate system. The key dimension
    represents the location of each coordinate along the x-axis, while
    the value dimension(s) represent the height of the area or the
    lower and upper bounds of the area between curves.

    Multiple areas may be stacked by overlaying them an passing them
    to the stack method.

    """

    group = param.String(default='Area', constant=True)

    @classmethod
    def stack(cls, areas, baseline_name='Baseline'):
        """Stacks an (Nd)Overlay of Area or Curve Elements by offsetting
        their baselines. To stack a HoloMap or DynamicMap use the map
        method.

        """
        if not len(areas):
            return areas
        is_overlay = isinstance(areas, Overlay)
        if is_overlay:
            areas = NdOverlay({i: el for i, el in enumerate(areas)})
        df = areas.dframe(multi_index=True)
        levels = list(range(areas.ndims))
        vdims = [[el.vdims[0], baseline_name] for el in areas]
        baseline = None
        stacked = areas.clone(shared_data=False)
        if len(levels) == 1:
            # Pandas 2.1 gives the following FutureWarning:
            #   Creating a Groupby object with a length-1 list-like level parameter
            #   will yield indexes as tuples in a future version.
            levels = levels[0]
        for (key, sdf), element_vdims in zip(df.groupby(level=levels, sort=False), vdims, strict=None):
            vdim = element_vdims[0]
            sdf = sdf.droplevel(levels).reindex(index=df.index.unique(-1), fill_value=0)
            if baseline is None:
                sdf[baseline_name] = 0
            else:
                sdf[vdim.name] = sdf[vdim.name] + baseline
                sdf[baseline_name] = baseline
            baseline = sdf[vdim.name]
            stacked[key] = areas[key].clone(sdf, vdims=element_vdims)
        return Overlay(stacked.values()) if is_overlay else stacked
