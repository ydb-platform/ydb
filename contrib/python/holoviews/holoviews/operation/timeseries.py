import numpy as np
import param

from ..core import Element, Operation
from ..core.data import PandasInterface
from ..core.util import _PANDAS_FUNC_LOOKUP
from ..element import Scatter


class RollingBase(param.Parameterized):
    """Parameters shared between `rolling` and `rolling_outlier_std`.

    """

    center = param.Boolean(default=True, doc="""
        Whether to set the x-coordinate at the center or right edge
        of the window.""")

    min_periods = param.Integer(default=None, allow_None=True, doc="""
       Minimum number of observations in window required to have a
       value (otherwise result is NaN).""")

    rolling_window = param.Integer(default=10, doc="""
        The window size over which to operate.""")

    def _roll_kwargs(self):
        return {'window': self.p.rolling_window,
                'center': self.p.center,
                'min_periods': self.p.min_periods}


class rolling(Operation,RollingBase):
    """Applies a function over a rolling window.

    """

    window_type = param.Selector(default=None, allow_None=True,
        objects=['boxcar', 'triang', 'blackman', 'hamming', 'bartlett',
                 'parzen', 'bohman', 'blackmanharris', 'nuttall',
                 'barthann', 'kaiser', 'gaussian', 'general_gaussian',
                 'slepian'], doc="The shape of the window to apply")

    function = param.Callable(default=np.mean, doc="""
        The function to apply over the rolling window.""")

    def _process_layer(self, element, key=None):
        xdim = element.kdims[0].name
        df = PandasInterface.as_dframe(element)
        df = df.set_index(xdim).rolling(win_type=self.p.window_type,
                                        **self._roll_kwargs())
        if self.p.window_type is None:
            rolled = df.apply(self.p.function, raw=True)
        elif self.p.function is np.mean:
            rolled = df.mean()
        elif self.p.function is np.sum:
            rolled = df.sum()
        else:
            raise ValueError("Rolling window function only supports "
                            "mean and sum when custom window_type is supplied")
        return element.clone(rolled.reset_index())

    def _process(self, element, key=None):
        return element.map(self._process_layer, Element)


class resample(Operation):
    """Resamples a timeseries of dates with a frequency and function.

    """

    closed = param.Selector(default=None, objects=['left', 'right'],
        doc="Which side of bin interval is closed", allow_None=True)

    function = param.Callable(default=np.mean, doc="""
        Function for computing new values out of existing ones.""")

    label = param.Selector(default='right', doc="""
        The bin edge to label the bin with.""")

    rule = param.String(default='D', doc="""
        A string representing the time interval over which to apply the resampling""")

    def _process_layer(self, element, key=None):
        df = PandasInterface.as_dframe(element)
        xdim = element.kdims[0].name
        resample_kwargs = {'rule': self.p.rule, 'label': self.p.label,
                           'closed': self.p.closed}
        df = df.set_index(xdim).resample(**resample_kwargs)
        fn = _PANDAS_FUNC_LOOKUP.get(self.p.function, self.p.function)
        return element.clone(df.apply(fn).reset_index())

    def _process(self, element, key=None):
        return element.map(self._process_layer, Element)


class rolling_outlier_std(Operation, RollingBase):
    """Detect outliers using the standard deviation within a rolling window.

    Outliers are the array elements outside `sigma` standard deviations from
    the smoothed trend line, as calculated from the trend line residuals.

    The rolling window is controlled by parameters shared with the
    `rolling` operation via the base class RollingBase, to make it
    simpler to use the same settings for both.

    """

    sigma = param.Number(default=2.0, doc="""
        Minimum sigma before a value is considered an outlier.""")

    def _process_layer(self, element, key=None):
        import pandas as pd
        ys = element.dimension_values(1)

        # Calculate the variation in the distribution of the residual
        avg = pd.Series(ys).rolling(**self._roll_kwargs()).mean()
        residual = ys - avg
        std = pd.Series(residual).rolling(**self._roll_kwargs()).std()

        # Get indices of outliers
        with np.errstate(invalid='ignore'):
            outliers = (np.abs(residual) > std * self.p.sigma).values
        return element[outliers].clone(new_type=Scatter)

    def _process(self, element, key=None):
        return element.map(self._process_layer, Element)
