"""
mquantiles function from scipy
scipy commit: 195569e91b0708aae26ea74581d03d3ca279117e

:author: Pierre GF Gerard-Marchant
:contact: pierregm_at_uga_edu

scipy is distributed under a BSD license

"""

import numpy as np
import numpy.ma as ma
from numpy.ma import masked, nomask

def mquantiles(a, prob=list([.25,.5,.75]), alphap=.4, betap=.4, axis=None,
               limit=()):
    """
    Computes empirical quantiles for a data array.

    Samples quantile are defined by ``Q(p) = (1-gamma)*x[j] + gamma*x[j+1]``,
    where ``x[j]`` is the j-th order statistic, and gamma is a function of
    ``j = floor(n*p + m)``, ``m = alphap + p*(1 - alphap - betap)`` and
    ``g = n*p + m - j``.

    Reinterpreting the above equations to compare to **R** lead to the
    equation: ``p(k) = (k - alphap)/(n + 1 - alphap - betap)``

    Typical values of (alphap,betap) are:
        - (0,1)    : ``p(k) = k/n`` : linear interpolation of cdf
          (**R** type 4)
        - (.5,.5)  : ``p(k) = (k - 1/2.)/n`` : piecewise linear function
          (**R** type 5)
        - (0,0)    : ``p(k) = k/(n+1)`` :
          (**R** type 6)
        - (1,1)    : ``p(k) = (k-1)/(n-1)``: p(k) = mode[F(x[k])].
          (**R** type 7, **R** default)
        - (1/3,1/3): ``p(k) = (k-1/3)/(n+1/3)``: Then p(k) ~ median[F(x[k])].
          The resulting quantile estimates are approximately median-unbiased
          regardless of the distribution of x.
          (**R** type 8)
        - (3/8,3/8): ``p(k) = (k-3/8)/(n+1/4)``: Blom.
          The resulting quantile estimates are approximately unbiased
          if x is normally distributed
          (**R** type 9)
        - (.4,.4)  : approximately quantile unbiased (Cunnane)
        - (.35,.35): APL, used with PWM

    Parameters
    ----------
    a : array_like
        Input data, as a sequence or array of dimension at most 2.
    prob : array_like, optional
        List of quantiles to compute.
    alphap : float, optional
        Plotting positions parameter, default is 0.4.
    betap : float, optional
        Plotting positions parameter, default is 0.4.
    axis : int, optional
        Axis along which to perform the trimming.
        If None (default), the input array is first flattened.
    limit : tuple
        Tuple of (lower, upper) values.
        Values of `a` outside this open interval are ignored.

    Returns
    -------
    mquantiles : MaskedArray
        An array containing the calculated quantiles.

    Notes
    -----
    This formulation is very similar to **R** except the calculation of
    ``m`` from ``alphap`` and ``betap``, where in **R** ``m`` is defined
    with each type.

    References
    ----------
    .. [1] *R* statistical software at http://www.r-project.org/

    Examples
    --------
    >>> from scipy.stats.mstats import mquantiles
    >>> a = np.array([6., 47., 49., 15., 42., 41., 7., 39., 43., 40., 36.])
    >>> mquantiles(a)
    array([ 19.2,  40. ,  42.8])

    Using a 2D array, specifying axis and limit.

    >>> data = np.array([[   6.,    7.,    1.],
                         [  47.,   15.,    2.],
                         [  49.,   36.,    3.],
                         [  15.,   39.,    4.],
                         [  42.,   40., -999.],
                         [  41.,   41., -999.],
                         [   7., -999., -999.],
                         [  39., -999., -999.],
                         [  43., -999., -999.],
                         [  40., -999., -999.],
                         [  36., -999., -999.]])
    >>> mquantiles(data, axis=0, limit=(0, 50))
    array([[ 19.2 ,  14.6 ,   1.45],
           [ 40.  ,  37.5 ,   2.5 ],
           [ 42.8 ,  40.05,   3.55]])

    >>> data[:, 2] = -999.
    >>> mquantiles(data, axis=0, limit=(0, 50))
    masked_array(data =
     [[19.2 14.6 --]
     [40.0 37.5 --]
     [42.8 40.05 --]],
                 mask =
     [[False False  True]
      [False False  True]
      [False False  True]],
           fill_value = 1e+20)

    """
    def _quantiles1D(data,m,p):
        x = np.sort(data.compressed())
        n = len(x)
        if n == 0:
            return ma.array(np.empty(len(p), dtype=float), mask=True)
        elif n == 1:
            return ma.array(np.resize(x, p.shape), mask=nomask)
        aleph = (n*p + m)
        k = np.floor(aleph.clip(1, n-1)).astype(int)
        gamma = (aleph-k).clip(0,1)
        return (1.-gamma)*x[(k-1).tolist()] + gamma*x[k.tolist()]

    # Initialization & checks ---------
    data = ma.array(a, copy=False)
    if data.ndim > 2:
        raise TypeError("Array should be 2D at most !")
    #
    if limit:
        condition = (limit[0] < data) & (data < limit[1])
        data[~condition.filled(True)] = masked
    #
    p = np.array(prob, copy=False, ndmin=1)
    m = alphap + p*(1.-alphap-betap)
    # Computes quantiles along axis (or globally)
    if (axis is None):
        return _quantiles1D(data, m, p)
    return ma.apply_along_axis(_quantiles1D, axis, data, m, p)
