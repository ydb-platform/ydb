import numpy as np
import param

from ..core import Dataset, Dimension, NdOverlay
from ..core.operation import Operation
from ..core.util import cartesian_product, isfinite
from ..element import Area, Bivariate, Contours, Curve, Distribution, Image, Polygons
from .element import contours


def _kde_support(bin_range, bw, gridsize, cut, clip):
    """Establish support for a kernel density estimate.

    """
    kmin, kmax = bin_range[0] - bw * cut, bin_range[1] + bw * cut
    if isfinite(clip[0]):
        kmin = max(kmin, clip[0])
    if isfinite(clip[1]):
        kmax = min(kmax, clip[1])
    return np.linspace(kmin, kmax, gridsize)


class univariate_kde(Operation):
    """Computes a 1D kernel density estimate (KDE) along the supplied
    dimension. Kernel density estimation is a non-parametric way to
    estimate the probability density function of a random variable.

    The KDE works by placing a Gaussian kernel at each sample with
    the supplied bandwidth. These kernels are then summed to produce
    the density estimate. By default a good bandwidth is determined
    using the bw_method but it may be overridden by an explicit value.

    """

    bw_method = param.Selector(default='scott', objects=['scott', 'silverman'], doc="""
        Method of automatically determining KDE bandwidth""")

    bandwidth = param.Number(default=None, doc="""
        Allows supplying explicit bandwidth value rather than relying on scott or silverman method.""")

    cut = param.Number(default=3, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    bin_range = param.NumericTuple(default=None, length=2,  doc="""
        Specifies the range within which to compute the KDE.""")

    dimension = param.String(default=None, doc="""
        Along which dimension of the Element to compute the KDE.""")

    filled = param.Boolean(default=True, doc="""
        Controls whether to return filled or unfilled KDE.""")

    n_samples = param.Integer(default=100, doc="""
        Number of samples to compute the KDE over.""")

    groupby = param.ClassSelector(default=None, class_=(str, Dimension), doc="""
      Defines a dimension to group the Histogram returning an NdOverlay of Histograms.""")

    _per_element = True

    def _process(self, element, key=None):
        if self.p.groupby:
            if not isinstance(element, Dataset):
                raise ValueError('Cannot use histogram groupby on non-Dataset Element')
            grouped = element.groupby(self.p.groupby, group_type=Dataset, container_type=NdOverlay)
            self.p.groupby = None
            return grouped.map(self._process, Dataset)

        try:
            from scipy import stats
            from scipy.linalg import LinAlgError
        except ImportError:
            raise ImportError(f'{type(self).__name__} operation requires SciPy to be installed.') from None

        params = {}
        if isinstance(element, Distribution):
            selected_dim = element.kdims[0]
            if element.group != type(element).__name__:
                params['group'] = element.group
            params['label'] = element.label
            vdim = element.vdims[0]
            vdim_name = f'{selected_dim.name}_density'
            vdims = [vdim.clone(vdim_name, label='Density') if vdim.name == 'Density' else vdim]
        else:
            if self.p.dimension:
                selected_dim = element.get_dimension(self.p.dimension)
            else:
                dimensions = element.vdims+element.kdims
                if not dimensions:
                    raise ValueError(f"{type(element).__name__} element does not declare any dimensions "
                                     "to compute the kernel density estimate on.")
                selected_dim = dimensions[0]
            vdim_name = f'{selected_dim.name}_density'
            vdims = [Dimension(vdim_name, label='Density')]

        data = element.dimension_values(selected_dim)
        bin_range = self.p.bin_range or element.range(selected_dim)
        if bin_range == (0, 0) or any(not isfinite(r) for r in bin_range):
            bin_range = (0, 1)
        elif bin_range[0] == bin_range[1]:
            bin_range = (bin_range[0]-0.5, bin_range[1]+0.5)

        element_type = Area if self.p.filled else Curve
        data = data[isfinite(data)] if len(data) else []
        if len(data) > 1:
            try:
                kde = stats.gaussian_kde(data)
            except LinAlgError:
                return element_type([], selected_dim, vdims, **params)
            if self.p.bandwidth:
                kde.set_bandwidth(self.p.bandwidth)
            bw = kde.scotts_factor() * data.std(ddof=1)
            if self.p.bin_range:
                xs = np.linspace(bin_range[0], bin_range[1], self.p.n_samples)
            else:
                xs = _kde_support(bin_range, bw, self.p.n_samples, self.p.cut, selected_dim.range)
            ys = kde.evaluate(xs)
        else:
            xs = np.linspace(bin_range[0], bin_range[1], self.p.n_samples)
            ys = np.full_like(xs, 0)

        return element_type((xs, ys), kdims=[selected_dim], vdims=vdims, **params)



class bivariate_kde(Operation):
    """Computes a 2D kernel density estimate (KDE) of the first two
    dimensions in the input data. Kernel density estimation is a
    non-parametric way to estimate the probability density function of
    a random variable.

    The KDE works by placing 2D Gaussian kernel at each sample with
    the supplied bandwidth. These kernels are then summed to produce
    the density estimate. By default a good bandwidth is determined
    using the bw_method but it may be overridden by an explicit value.

    """

    contours = param.Boolean(default=True, doc="""
        Whether to compute contours from the KDE, determines whether to
        return an Image or Contours/Polygons.""")

    bw_method = param.Selector(default='scott', objects=['scott', 'silverman'], doc="""
        Method of automatically determining KDE bandwidth""")

    bandwidth = param.Number(default=None, doc="""
        Allows supplying explicit bandwidth value rather than relying
        on scott or silverman method.""")

    cut = param.Number(default=3, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    filled = param.Boolean(default=False, doc="""
        Controls whether to return filled or unfilled contours.""")

    levels = param.ClassSelector(default=10, class_=(list, int), doc="""
        A list of scalar values used to specify the contour levels.""")

    n_samples = param.Integer(default=100, doc="""
        Number of samples to compute the KDE over.""")

    x_range  = param.NumericTuple(default=None, length=2, doc="""
       The x_range as a tuple of min and max x-value. Auto-ranges
       if set to None.""")

    y_range  = param.NumericTuple(default=None, length=2, doc="""
       The x_range as a tuple of min and max y-value. Auto-ranges
       if set to None.""")

    _per_element = True

    def _process(self, element, key=None):
        try:
            from scipy import stats
        except ImportError:
            raise ImportError(f'{type(self).__name__} operation requires SciPy to be installed.') from None

        if len(element.dimensions()) < 2:
            raise ValueError("bivariate_kde can only be computed on elements "
                             "declaring at least two dimensions.")
        xdim, ydim = element.dimensions()[:2]
        params = {}
        if isinstance(element, Bivariate):
            if element.group != type(element).__name__:
                params['group'] = element.group
            params['label'] = element.label
            vdim = element.vdims[0]
        else:
            vdim = 'Density'

        data = element.array([0, 1]).T
        xmin, xmax = self.p.x_range or element.range(0)
        ymin, ymax = self.p.y_range or element.range(1)
        if any(not isfinite(v) for v in (xmin, xmax)):
            xmin, xmax = -0.5, 0.5
        elif xmin == xmax:
            xmin, xmax = xmin-0.5, xmax+0.5
        if any(not isfinite(v) for v in (ymin, ymax)):
            ymin, ymax = -0.5, 0.5
        elif ymin == ymax:
            ymin, ymax = ymin-0.5, ymax+0.5

        data = data[:, isfinite(data).min(axis=0)] if data.shape[1] > 1 else np.empty((2, 0))
        if data.shape[1] > 1:
            kde = stats.gaussian_kde(data)
            if self.p.bandwidth:
                kde.set_bandwidth(self.p.bandwidth)
            bw = kde.scotts_factor() * data.std(ddof=1)
            if self.p.x_range:
                xs = np.linspace(xmin, xmax, self.p.n_samples)
            else:
                xs = _kde_support((xmin, xmax), bw, self.p.n_samples, self.p.cut, xdim.range)
            if self.p.y_range:
                ys = np.linspace(ymin, ymax, self.p.n_samples)
            else:
                ys = _kde_support((ymin, ymax), bw, self.p.n_samples, self.p.cut, ydim.range)
            xx, yy = cartesian_product([xs, ys], False)
            positions = np.vstack([xx.ravel(), yy.ravel()])
            f = np.reshape(kde(positions).T, xx.shape)
        elif self.p.contours:
            eltype = Polygons if self.p.filled else Contours
            return eltype([], kdims=[xdim, ydim], vdims=[vdim])
        else:
            xs = np.linspace(xmin, xmax, self.p.n_samples)
            ys = np.linspace(ymin, ymax, self.p.n_samples)
            f = np.zeros((self.p.n_samples, self.p.n_samples))

        img = Image((xs, ys, f.T), kdims=element.dimensions()[:2], vdims=[vdim], **params)
        if self.p.contours:
            cntr = contours(img, filled=self.p.filled, levels=self.p.levels)
            return cntr.clone(cntr.data[1:], **params)
        return img
