import numpy as np
import param

from .element import ColorbarPlot


class HexTilesPlot(ColorbarPlot):

    aggregator = param.Callable(default=np.size, doc="""
      Aggregation function used to compute bin values. Any NumPy
      reduction is allowed, defaulting to np.size to count the number
      of values in each bin.""")

    gridsize = param.ClassSelector(default=50, class_=(int, tuple), doc="""
      Number of hexagonal bins along x- and y-axes. Defaults to uniform
      sampling along both axes when setting and integer but independent
      bin sampling can be specified a tuple of integers corresponding to
      the number of bins along each axis.""")

    max_scale = param.Number(default=0.9, bounds=(0, None), doc="""
      When size_index is enabled this defines the maximum size of each
      bin relative to uniform tile size, i.e. for a value of 1, the
      largest bin will match the size of bins when scaling is disabled.
      Setting value larger than 1 will result in overlapping bins.""")

    min_count = param.Number(default=None, doc="""
      The display threshold before a bin is shown, by default bins with
      a count of less than 1 are hidden.""")

    style_opts = ['cmap', 'edgecolors', 'alpha', 'linewidths', 'marginals']

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='hexbin')

    def get_data(self, element, ranges, style):
        if not element.vdims:
            element = element.add_dimension('Count', 0, np.ones(len(element)), True)
        xs, ys = (element.dimension_values(i) for i in range(2))
        args = (ys, xs) if self.invert_axes else (xs, ys)
        args += (element.dimension_values(2),)

        cdim = element.vdims[0]
        agg = np.sum if self.aggregator is np.size else self.aggregator
        self._norm_kwargs(element, ranges, style, cdim)
        style['reduce_C_function'] = agg
        style['vmin'], style['vmax'] = cdim.range
        style['xscale'] = 'log' if self.logx else 'linear'
        style['yscale'] = 'log' if self.logy else 'linear'
        style['gridsize'] = self.gridsize
        style['mincnt'] = -1 if self.min_count == 0 else self.min_count
        return args, style, {}
