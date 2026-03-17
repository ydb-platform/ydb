import numpy as np
import param

from ...core.ndmapping import sorted_context
from ..mixins import MultiDistributionMixin
from .chart import AreaPlot, ChartPlot
from .path import PolygonPlot
from .plot import AdjoinedPlot
from .util import MPL_GE_3_9_0, MPL_GE_3_10_0


class DistributionPlot(AreaPlot):
    """DistributionPlot visualizes a distribution of values as a KDE.

    """

    bandwidth = param.Number(default=None, doc="""
        The bandwidth of the kernel for the density estimate.""")

    cut = param.Number(default=3, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    filled = param.Boolean(default=True, doc="""
        Whether the bivariate contours should be filled.""")


class BivariatePlot(PolygonPlot):
    """Bivariate plot visualizes two-dimensional kernel density
    estimates. Additionally, by enabling the joint option, the
    marginals distributions can be plotted alongside each axis (does
    not animate or compose).

    """

    bandwidth = param.Number(default=None, doc="""
        The bandwidth of the kernel for the density estimate.""")

    cut = param.Number(default=3, doc="""
        Draw the estimate to cut * bw from the extreme data points.""")

    filled = param.Boolean(default=False, doc="""
        Whether the bivariate contours should be filled.""")

    levels = param.ClassSelector(default=10, class_=(list, int), doc="""
        A list of scalar values used to specify the contour levels.""")


class BoxPlot(MultiDistributionMixin, ChartPlot):
    """BoxPlot plots the ErrorBar Element type and supporting
    both horizontal and vertical error bars via the 'horizontal'
    plot option.

    """

    style_opts = ['notch', 'sym', 'whis', 'bootstrap',  # typos: ignore
                  'conf_intervals', 'widths', 'showmeans',
                  'show_caps', 'showfliers', 'boxprops',
                  'whiskerprops', 'capprops', 'flierprops',
                  'medianprops', 'meanprops', 'meanline']

    _nonvectorized_styles = style_opts

    _plot_methods = dict(single='boxplot')

    def get_data(self, element, ranges, style):
        if element.kdims:
            with sorted_context(False):
                groups = element.groupby(element.kdims).data.items()
        else:
            groups = [(element.label, element)]

        data, labels = [], []
        for key, group in groups:
            if element.kdims:
                label = ','.join([d.pprint_value(v) for d, v in zip(element.kdims, key, strict=None)])
            else:
                label = key
            d = group[group.vdims[0]]
            data.append(d[np.isfinite(d)])
            labels.append(label)
        if MPL_GE_3_9_0:
            style['tick_labels'] = labels
        else:
            style['labels'] = labels
        style = {k: v for k, v in style.items()
                 if k not in ['zorder', 'label']}
        if MPL_GE_3_10_0:
            style["orientation"] = "horizontal" if self.invert_axes else "vertical"
        else:
            style["vert"] = not self.invert_axes
        format_kdims = [kd.clone(value_format=None) for kd in element.kdims]
        return (data,), style, {'dimensions': [format_kdims, element.vdims[0]]}

    def init_artists(self, ax, plot_args, plot_kwargs):
        artists = ax.boxplot(*plot_args, **plot_kwargs)
        artists['artist'] = artists['boxes']
        return artists

    def teardown_handles(self):
        for g in ('whiskers', 'fliers', 'medians', 'boxes', 'caps', 'means'):
            for v in self.handles.get(g, []):
                v.remove()


class SideBoxPlot(AdjoinedPlot, BoxPlot):

    bgcolor = param.Parameter(default=(1, 1, 1, 0), doc="""
        Make plot background invisible.""")

    border_size = param.Number(default=0, doc="""
        The size of the border expressed as a fraction of the main plot.""")

    xaxis = param.Selector(default='bare',
                                 objects=['top', 'bottom', 'bare', 'top-bare',
                                          'bottom-bare', None], doc="""
        Whether and where to display the xaxis, bare options allow suppressing
        all axis labels including ticks and xlabel. Valid options are 'top',
        'bottom', 'bare', 'top-bare' and 'bottom-bare'.""")

    yaxis = param.Selector(default='bare',
                                 objects=['left', 'right', 'bare', 'left-bare',
                                          'right-bare', None], doc="""
        Whether and where to display the yaxis, bare options allow suppressing
        all axis labels including ticks and ylabel. Valid options are 'left',
        'right', 'bare' 'left-bare' and 'right-bare'.""")

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if self.adjoined:
            self.invert_axes = not self.invert_axes


class ViolinPlot(BoxPlot):
    """BoxPlot plots the ErrorBar Element type and supporting
    both horizontal and vertical error bars via the 'horizontal'
    plot option.

    """

    bandwidth = param.Number(default=None, doc="""
        Allows supplying explicit bandwidth value rather than relying
        on scott or silverman method.""")

    inner = param.Selector(objects=['box', 'medians', None],
                                 default='box', doc="""
        Inner visual indicator for distribution values:

          * box - A small box plot
          * stick - Lines indicating each sample value
          * quartiles - Indicates first, second and third quartiles
        """)

    _plot_methods = dict(single='violinplot')

    style_opts = ['showmeans', 'facecolors', 'showextrema', 'bw_method',
                  'widths', 'stats_color', 'box_color', 'alpha', 'edgecolors']

    _nonvectorized_styles = [
        s for s in style_opts if s not in ('facecolors', 'edgecolors', 'widths')
    ]

    def init_artists(self, ax, plot_args, plot_kwargs):
        box_color = plot_kwargs.pop('box_color', 'black')
        stats_color = plot_kwargs.pop('stats_color', 'black')
        facecolors = plot_kwargs.pop('facecolors', [])
        edgecolors = plot_kwargs.pop('edgecolors', 'black')
        if MPL_GE_3_9_0:
            labels = {'tick_labels': plot_kwargs.pop('tick_labels')}
        else:
            labels = {'labels': plot_kwargs.pop('labels')}
        alpha = plot_kwargs.pop('alpha', 1.)
        showmedians = self.inner == 'medians'
        bw_method = self.bandwidth or 'scott'
        artists = ax.violinplot(*plot_args, bw_method=bw_method,
                               showmedians=showmedians, **plot_kwargs)
        if self.inner == 'box':
            if MPL_GE_3_10_0:
                invert_axes = {"orientation": "horizontal" if self.invert_axes else "vertical"}
            else:
                invert_axes = {"vert": not self.invert_axes}
            box = ax.boxplot(*plot_args, positions=plot_kwargs['positions'],
                             showfliers=False, showcaps=False, patch_artist=True,
                             boxprops={'facecolor': box_color},
                             medianprops={'color': 'white'}, widths=0.1,
                             **invert_axes, **labels)
            artists.update(box)
        for body, color in zip(artists['bodies'], facecolors, strict=None):
            body.set_facecolors(color)
            body.set_edgecolors(edgecolors)
            body.set_alpha(alpha)
        for stat in ['cmedians', 'cmeans', 'cmaxes', 'cmins', 'cbars']:
            if stat in artists:
                artists[stat].set_edgecolors(stats_color)
        artists['bodies'] = artists['bodies']
        return artists

    def get_data(self, element, ranges, style):
        if element.kdims:
            with sorted_context(False):
                groups = element.groupby(element.kdims).data.items()
        else:
            groups = [(element.label, element)]

        data, labels, colors = [], [], []
        elstyle = self.lookup_options(element, 'style')
        for i, (key, group) in enumerate(groups):
            if element.kdims:
                label = ','.join([d.pprint_value(v) for d, v in zip(element.kdims, key, strict=None)])
            else:
                label = key
            d = group[group.vdims[0]]
            data.append(d[np.isfinite(d)])
            labels.append(label)
            colors.append(elstyle[i].get('facecolors', 'blue'))
        style['positions'] = list(range(len(data)))
        if MPL_GE_3_9_0:
            style['tick_labels'] = labels
        else:
            style['labels'] = labels
        style['facecolors'] = colors

        if element.ndims > 0:
            element = element.aggregate(function=np.mean)
        else:
            element = element.clone([(element.aggregate(function=np.mean),)])

        new_style = self._apply_transforms(element, ranges, style)
        style = {k: v for k, v in new_style.items()
                 if k not in ['zorder', 'label']}
        if MPL_GE_3_10_0:
            style["orientation"] = "horizontal" if self.invert_axes else "vertical"
        else:
            style["vert"] = not self.invert_axes
        format_kdims = [kd.clone(value_format=None) for kd in element.kdims]
        ticks = {'yticks' if self.invert_axes else 'xticks': list(enumerate(labels))}
        return (data,), style, dict(dimensions=[format_kdims, element.vdims[0]], **ticks)

    def teardown_handles(self):
        box_artists = ('cmedians', 'cmeans', 'cmaxes', 'cmins', 'cbars', 'bodies')
        violin_artists = ('whiskers', 'fliers', 'medians', 'boxes', 'caps', 'means')
        for group in box_artists+violin_artists:
            for v in self.handles.get(group, []):
                v.remove()
