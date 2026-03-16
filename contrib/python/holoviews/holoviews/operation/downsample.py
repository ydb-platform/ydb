"""Implements downsampling algorithms for large 1D datasets.

The algorithms implemented in this module have been adapted from
https://github.com/predict-idlab/plotly-resampler and are reproduced
along with the original license:

MIT License

Copyright (c) 2022 Jonas Van Der Donckt, Jeroen Van Der Donckt, Emiel Deprost.

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.

"""

import math
from functools import partial

import numpy as np
import param

from ..core import NdOverlay, Overlay
from ..core.util import dtype_kind
from ..element.chart import Area
from .resample import ResampleOperation1D


def _argmax_area(prev_x, prev_y, avg_next_x, avg_next_y, x_bucket, y_bucket):
    """Vectorized triangular area argmax computation.

    Parameters
    ----------
    prev_x : float
        The previous selected point is x value.
    prev_y : float
        The previous selected point its y value.
    avg_next_x : float
        The x mean of the next bucket
    avg_next_y : float
        The y mean of the next bucket
    x_bucket : np.ndarray
        All x values in the bucket
    y_bucket : np.ndarray
        All y values in the bucket

    Returns
    -------
    int
        The index of the point with the largest triangular area.
    """
    return np.abs(
        x_bucket * (prev_y - avg_next_y)
        + y_bucket * (avg_next_x - prev_x)
        + (prev_x * avg_next_y - avg_next_x * prev_y)
    ).argmax()


def _lttb_inner(x, y, n_out, sampled_x, offset):
    a = 0
    for i in range(n_out - 3):
        o0, o1, o2 = offset[i], offset[i + 1], offset[i + 2]
        a = (
            _argmax_area(
                x[a],
                y[a],
                x[o1:o2].mean(),
                y[o1:o2].mean(),
                x[o0:o1],
                y[o0:o1],
            )
            + offset[i]
        )
        sampled_x[i + 1] = a

    # ------------ EDGE CASE ------------
    # next-average of last bucket = last point
    sampled_x[-2] = (
        _argmax_area(
            x[a],
            y[a],
            x[-1],  # last point
            y[-1],
            x[offset[-2] : offset[-1]],
            y[offset[-2] : offset[-1]],
        )
        + offset[-2]
    )


def _ensure_contiguous(x, y):
    """Ensures the arrays are contiguous in memory (required by tsdownsample).

    """
    return np.ascontiguousarray(x), np.ascontiguousarray(y)


def _lttb(x, y, n_out, **kwargs):
    """Downsample the data using the LTTB algorithm.

    Will use a Python/Numpy implementation if tsdownsample is not available.

    Parameters
    ----------
    x : np.ndarray
        The x-values of the data.
    y : np.ndarray
        The y-values of the data.
    n_out : int
        The number of output points.

    Returns
    -------
    np.array: The indexes of the selected datapoints.
    """
    try:
        from tsdownsample import LTTBDownsampler
        x, y = _ensure_contiguous(x, y)
        return LTTBDownsampler().downsample(x, y, n_out=n_out, **kwargs)
    except ModuleNotFoundError:
        pass
    except Exception as e:
        raise e

    # Bucket size. Leave room for start and end data points
    block_size = (y.shape[0] - 2) / (n_out - 2)
    # Note this 'astype' cast must take place after array creation (and not with the
    # aranage() its dtype argument) or it will cast the `block_size` step to an int
    # before the arange array creation
    offset = np.arange(start=1, stop=y.shape[0], step=block_size).astype(np.int64)

    # Construct the output array
    sampled_x = np.empty(n_out, dtype=np.int64)
    sampled_x[0] = 0
    sampled_x[-1] = x.shape[0] - 1

    # View it as int64 to take the mean of it
    if dtype_kind(x) == 'M':
        x = x.view(np.int64)
    if dtype_kind(y) == 'M':
        y = y.view(np.int64)

    _lttb_inner(x, y, n_out, sampled_x, offset)

    return sampled_x

def _nth_point(x, y, n_out, **kwargs):
    """Downsampling by selecting every n-th datapoint

    Parameters
    ----------
    x : np.ndarray
        The x-values of the data.
    y : np.ndarray
        The y-values of the data.
    n_out : int
        The number of output points.

    Returns
    -------
    slice : The slice of selected datapoints.
    """
    n_samples = len(x)
    return slice(0, n_samples, max(1, math.ceil(n_samples / n_out)))

def _viewport(x, y, n_out, **kwargs):
    return slice(len(x))

def _min_max(x, y, n_out, **kwargs):
    try:
        from tsdownsample import MinMaxDownsampler
    except ModuleNotFoundError:
        raise NotImplementedError(
            'The min-max downsampling algorithm requires the tsdownsample '
            'library to be installed.'
        ) from None
    x, y = _ensure_contiguous(x, y)
    return MinMaxDownsampler().downsample(x, y, n_out=n_out, **kwargs)

def _min_max_lttb(x, y, n_out, **kwargs):
    try:
        from tsdownsample import MinMaxLTTBDownsampler
    except ModuleNotFoundError:
        raise NotImplementedError(
            'The minmax-lttb downsampling algorithm requires the tsdownsample '
            'library to be installed.'
        ) from None
    x, y = _ensure_contiguous(x, y)
    return MinMaxLTTBDownsampler().downsample(x, y, n_out=n_out, **kwargs)

def _m4(x, y, n_out, **kwargs):
    try:
        from tsdownsample import M4Downsampler
    except ModuleNotFoundError:
        raise NotImplementedError(
            'The m4 downsampling algorithm requires the tsdownsample '
            'library to be installed.'
        ) from None
    x, y = _ensure_contiguous(x, y)
    n_out = n_out - (n_out % 4)  # n_out must be a multiple of 4
    return M4Downsampler().downsample(x, y, n_out=n_out, **kwargs)


_ALGORITHMS = {
    'lttb': _lttb,
    'nth': _nth_point,
    'viewport': _viewport,
    'minmax': _min_max,
    'minmax-lttb': _min_max_lttb,
    'm4': _m4,
}

class downsample1d(ResampleOperation1D):
    """Implements downsampling of a regularly sampled 1D dataset.

    If available uses the `tsdownsample` library to perform massively
    accelerated downsampling.

    """

    algorithm = param.Selector(default='lttb', objects=list(_ALGORITHMS), doc="""
        The algorithm to use for downsampling:

        - `lttb`: Largest Triangle Three Buckets downsample algorithm.
        - `nth`: Selects every n-th point.
        - `viewport`: Selects all points in a given viewport.
        - `minmax`: Selects the min and max value in each bin (requires tsdownsample).
        - `m4`: Selects the min, max, first and last value in each bin (requires tsdownsample).
        - `minmax-lttb`: First selects n_out * minmax_ratio min and max values,
                         then further reduces these to n_out values using the
                         Largest Triangle Three Buckets algorithm (requires tsdownsample).""")

    parallel = param.Boolean(default=False, doc="""
       The number of threads to use (if tsdownsample is available).""")

    minmax_ratio = param.Integer(default=4, bounds=(0, None), doc="""
       For the minmax-lttb algorithm determines the ratio of candidate
       values to generate with the minmax algorithm before further
       downsampling with LTTB.""")

    neighbor_points = param.Boolean(default=None, doc="""
        Whether to add the neighbor points to the range before downsampling.
        By default this is only enabled for the viewport algorithm.""")

    def _process(self, element, key=None, shared_data=None):
        if isinstance(element, (Overlay, NdOverlay)):
            # Shared data is so we only slice the given data once
            kwargs = {'key': key, 'shared_data': {}}
            _process = partial(self._process, **kwargs)
            if isinstance(element, Overlay):
                elements = [v.map(_process) for v in element]
            else:
                elements = {k: v.map(_process) for k, v in element.items()}
            return element.clone(elements)

        if self.p.x_range:
            key = (id(element.data), str(element.kdims[0]))
            if shared_data is not None and key in shared_data:
                element = element.clone(shared_data[key])
            else:
                mask = self._compute_mask(element)
                element = element[mask]
                if shared_data is not None:
                    shared_data[key] = element.data

        if len(element) <= self.p.width:
            return element
        xs, ys = (element.dimension_values(i) for i in range(2))
        if ys.dtype == np.bool_:
            ys = ys.astype(np.int8)
        downsample = _ALGORITHMS[self.p.algorithm]
        kwargs = {}
        if "lttb" in self.p.algorithm and isinstance(element, Area):
            raise NotImplementedError(
                "LTTB algorithm is not implemented for hv.Area"
            )
        elif self.p.algorithm == "minmax-lttb":
            kwargs['minmax_ratio'] = self.p.minmax_ratio
        samples = downsample(xs, ys, self.p.width, parallel=self.p.parallel, **kwargs)
        return element.iloc[samples]

    def _compute_mask(self, element):
        """Computes the mask to apply to the element before downsampling.

        """
        neighbor_enabled = (
            self.p.neighbor_points
            if self.p.neighbor_points is not None
            else self.p.algorithm == "viewport"
        )
        if not neighbor_enabled:
            return slice(*self.p.x_range)
        try:
            mask = element.dataset.interface._select_mask_neighbor(
                element.dataset, {element.kdims[0]: self.p.x_range}
            )
        except NotImplementedError:
            mask = slice(*self.p.x_range)
        except Exception as e:
            self.param.warning(f"Could not apply neighbor mask to downsample1d: {e}")
            mask = slice(*self.p.x_range)
        return mask
