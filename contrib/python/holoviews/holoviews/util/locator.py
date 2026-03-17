"""Minimal set of functionality of Matplotlib's MaxNLocator to choose contour
levels without having to have Matplotlib installed.
Taken from Matplotlib 3.8.0.

"""
import math

import numpy as np


class _Edge_integer:
    """Helper for `.MaxNLocator`, `.MultipleLocator`, etc.

    Take floating-point precision limitations into account when calculating
    tick locations as integer multiples of a step.

    """

    def __init__(self, step, offset):
        """
        Parameters
        ----------
        step : float > 0
            Interval between ticks.
        offset : float
            Offset subtracted from the data limits prior to calculating tick
            locations.
        """
        if step <= 0:
            raise ValueError("'step' must be positive")
        self.step = step
        self._offset = abs(offset)

    def closeto(self, ms, edge):
        # Allow more slop when the offset is large compared to the step.
        if self._offset > 0:
            digits = np.log10(self._offset / self.step)
            tol = max(1e-10, 10 ** (digits - 12))
            tol = min(0.4999, tol)
        else:
            tol = 1e-10
        return abs(ms - edge) < tol

    def le(self, x):
        """Return the largest n: n*step <= x.

        """
        d, m = divmod(x, self.step)
        if self.closeto(m / self.step, 1):
            return d + 1
        return d

    def ge(self, x):
        """Return the smallest n: n*step >= x.

        """
        d, m = divmod(x, self.step)
        if self.closeto(m / self.step, 0):
            return d
        return d + 1


def nonsingular(vmin, vmax, expander=0.001, tiny=1e-15, increasing=True):
    """Modify the endpoints of a range as needed to avoid singularities.

    Parameters
    ----------
    vmin, vmax : float
        The initial endpoints.
    expander : float, default: 0.001
        Fractional amount by which *vmin* and *vmax* are expanded if
        the original interval is too small, based on *tiny*.
    tiny : float, default: 1e-15
        Threshold for the ratio of the interval to the maximum absolute
        value of its endpoints.  If the interval is smaller than
        this, it will be expanded.  This value should be around
        1e-15 or larger; otherwise the interval will be approaching
        the double precision resolution limit.
    increasing : bool, default: True
        If True, swap *vmin*, *vmax* if *vmin* > *vmax*.

    Returns
    -------
    vmin, vmax : float
        Endpoints, expanded and/or swapped if necessary.
        If either input is inf or NaN, or if both inputs are 0 or very
        close to zero, it returns -*expander*, *expander*.
    """
    if (not np.isfinite(vmin)) or (not np.isfinite(vmax)):
        return -expander, expander

    swapped = False
    if vmax < vmin:
        vmin, vmax = vmax, vmin
        swapped = True

    # Expand vmin, vmax to float: if they were integer types, they can wrap
    # around in abs (abs(np.int8(-128)) == -128) and vmax - vmin can overflow.
    vmin, vmax = map(float, [vmin, vmax])

    maxabsvalue = max(abs(vmin), abs(vmax))
    if maxabsvalue < (1e6 / tiny) * np.finfo(float).tiny:
        vmin = -expander
        vmax = expander

    elif vmax - vmin <= maxabsvalue * tiny:
        if vmax == 0 and vmin == 0:
            vmin = -expander
            vmax = expander
        else:
            vmin -= expander*abs(vmin)
            vmax += expander*abs(vmax)

    if swapped and not increasing:
        vmin, vmax = vmax, vmin
    return vmin, vmax


def scale_range(vmin, vmax, n=1, threshold=100):
    dv = abs(vmax - vmin)  # > 0 as nonsingular is called before.
    meanv = (vmax + vmin) / 2
    if abs(meanv) / dv < threshold:
        offset = 0
    else:
        offset = math.copysign(10 ** (math.log10(abs(meanv)) // 1), meanv)
    scale = 10 ** (math.log10(dv / n) // 1)
    return scale, offset


class MaxNLocator:
    _extended_steps = np.array([
        0.1, 0.15, 0.2, 0.25, 0.3, 0.4, 0.5, 0.6, 0.8,
        1., 1.5, 2., 2.5, 3., 4., 5., 6., 8., 10., 15.])
    _min_n_ticks = 1

    def __init__(self, nbins: int = 10):
        if nbins < 1:
            raise ValueError("MaxNLocator nbins must be an integer greater than zero")
        self.nbins = nbins

    def _raw_ticks(self, vmin, vmax):
        scale, offset = scale_range(vmin, vmax, self.nbins)
        _vmin = vmin - offset
        _vmax = vmax - offset
        steps = self._extended_steps * scale

        raw_step = ((_vmax - _vmin) / self.nbins)
        large_steps = steps >= raw_step

        # Find index of smallest large step
        istep = np.nonzero(large_steps)[0][0]

        # Start at smallest of the steps greater than the raw step, and check
        # if it provides enough ticks. If not, work backwards through
        # smaller steps until one is found that provides enough ticks.
        for step in steps[:istep+1][::-1]:
            best_vmin = (_vmin // step) * step

            # Find tick locations spanning the vmin-vmax range, taking into
            # account degradation of precision when there is a large offset.
            # The edge ticks beyond vmin and/or vmax are needed for the
            # "round_numbers" autolimit mode.
            edge = _Edge_integer(step, offset)
            low = edge.le(_vmin - best_vmin)
            high = edge.ge(_vmax - best_vmin)
            ticks = np.arange(low, high + 1) * step + best_vmin
            # Count only the ticks that will be displayed.
            nticks = ((ticks <= _vmax) & (ticks >= _vmin)).sum()
            if nticks >= self._min_n_ticks:
                break
        return ticks + offset

    def tick_values(self, vmin, vmax):
        vmin, vmax = nonsingular(vmin, vmax, expander=1e-13, tiny=1e-14)
        locs = self._raw_ticks(vmin, vmax)
        return locs
