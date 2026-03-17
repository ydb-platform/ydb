import matplotlib as mpl
import numpy as np
import param
from matplotlib import patches
from matplotlib.lines import Line2D

from ...core.options import abbreviated_exception
from ...core.util import dtype_kind, match_spec
from ...element import HLines, HSpans, VLines, VSpans
from .element import ColorbarPlot, ElementPlot
from .plot import mpl_rc_context


class ABLine2D(Line2D):
    """Draw a line based on its slope and y-intercept.

    Additional arguments are passed to the matplotlib.lines.Line2D constructor.
    """

    def __init__(self, slope, intercept, *args, **kwargs):
        ax = kwargs['axes']

        # init the line, add it to the axes
        super().__init__([], [], *args, **kwargs)
        self._slope = slope
        self._intercept = intercept
        ax.add_line(self)

        # cache the renderer, draw the line for the first time
        ax.figure.canvas.draw()
        self._update_lim(None)

        # connect to axis callbacks
        self.axes.callbacks.connect('xlim_changed', self._update_lim)
        self.axes.callbacks.connect('ylim_changed', self._update_lim)

    def _update_lim(self, event):
        """Called whenever axis x/y limits change."""
        x = np.array(self.axes.get_xbound())
        y = (self._slope * x) + self._intercept
        self.set_data(x, y)
        self.axes.draw_artist(self)


class AnnotationPlot(ElementPlot):
    """AnnotationPlot handles the display of all annotation elements.

    """

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    def __init__(self, annotation, **params):
        self._annotation = annotation
        super().__init__(annotation, **params)
        self.handles['annotations'] = []

    @mpl_rc_context
    def initialize_plot(self, ranges=None):
        annotation = self.hmap.last
        key = self.keys[-1]
        ranges = self.compute_ranges(self.hmap, key, ranges)
        ranges = match_spec(annotation, ranges)
        axis = self.handles['axis']
        opts = self.style[self.cyclic_index]
        with abbreviated_exception():
            handles = self.draw_annotation(axis, annotation.data, opts)
        self.handles['annotations'] = handles
        return self._finalize_axis(key, element=annotation, ranges=ranges)

    def update_handles(self, key, axis, annotation, ranges, style):
        # Clear all existing annotations
        for element in self.handles['annotations']:
            element.remove()

        with abbreviated_exception():
            self.handles['annotations'] = self.draw_annotation(axis, annotation.data, style)


class VLinePlot(AnnotationPlot):
    """Draw a vertical line on the axis

    """

    style_opts = ['alpha', 'color', 'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, position, opts):
        if self.invert_axes:
            return [axis.axhline(position, **opts)]
        else:
            return [axis.axvline(position, **opts)]


class HLinePlot(AnnotationPlot):
    """Draw a horizontal line on the axis

    """

    style_opts = ['alpha', 'color', 'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, position, opts):
        """Draw a horizontal line on the axis

        """
        if self.invert_axes:
            return [axis.axvline(position, **opts)]
        else:
            return [axis.axhline(position, **opts)]


class VSpanPlot(AnnotationPlot):
    """Draw a vertical span on the axis

    """

    style_opts = ['alpha', 'color', 'facecolor', 'edgecolor',
                  'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, positions, opts):
        """Draw a vertical span on the axis

        """
        if self.invert_axes:
            return [axis.axhspan(*positions, **opts)]
        else:
            return [axis.axvspan(*positions, **opts)]


class HSpanPlot(AnnotationPlot):
    """Draw a horizontal span on the axis

    """

    style_opts = ['alpha', 'color', 'facecolor', 'edgecolor',
                  'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, positions, opts):
        """Draw a horizontal span on the axis

        """
        if self.invert_axes:
            return [axis.axvspan(*positions, **opts)]
        else:
            return [axis.axhspan(*positions, **opts)]


class SlopePlot(AnnotationPlot):

    style_opts = ['alpha', 'color', 'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, position, opts):
        """Draw a horizontal line on the axis

        """
        gradient, intercept = position
        if self.invert_axes:
            if gradient == 0:
                gradient = np.inf, np.inf
            else:
                gradient, intercept = 1/gradient, -(intercept/gradient)
        artist = ABLine2D(*position, axes=axis, **opts)
        return [artist]


class TextPlot(AnnotationPlot):
    """Draw the Text annotation object

    """

    style_opts = ['alpha', 'color', 'family', 'weight', 'visible']

    def draw_annotation(self, axis, data, opts):
        (x,y, text, fontsize,
         horizontalalignment, verticalalignment, rotation) = data
        if self.invert_axes: x, y = y, x
        opts['fontsize'] = fontsize
        return [axis.text(x,y, text,
                          horizontalalignment = horizontalalignment,
                          verticalalignment = verticalalignment,
                          rotation=rotation, **opts)]


class LabelsPlot(ColorbarPlot):

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
      Index of the dimension from which the color will the drawn""")

    xoffset = param.Number(default=None, doc="""
      Amount of offset to apply to labels along x-axis.""")

    yoffset = param.Number(default=None, doc="""
      Amount of offset to apply to labels along x-axis.""")

    style_opts = ['alpha', 'color', 'family', 'weight', 'size', 'visible',
                  'horizontalalignment', 'verticalalignment', 'cmap', 'rotation']

    _nonvectorized_styles = ['cmap']

    _plot_methods = dict(single='annotate')

    def get_data(self, element, ranges, style):
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        xs, ys = (element.dimension_values(i) for i in range(2))
        tdim = element.get_dimension(2)
        text = [tdim.pprint_value(v) for v in element.dimension_values(tdim)]
        positions = (ys, xs) if self.invert_axes else (xs, ys)
        if self.xoffset is not None:
            xs += self.xoffset
        if self.yoffset is not None:
            ys += self.yoffset

        cs = None
        cdim = element.get_dimension(self.color_index)
        if cdim:
            self._norm_kwargs(element, ranges, style, cdim)
            cs = element.dimension_values(cdim)
        if 'c' in style:
            cs = style.pop('c')

        if 'size' in style: style['fontsize'] = style.pop('size')
        if 'horizontalalignment' not in style: style['horizontalalignment'] = 'center'
        if 'verticalalignment' not in style: style['verticalalignment'] = 'center'
        return (*positions, text, cs), style, {}

    def init_artists(self, ax, plot_args, plot_kwargs):
        if plot_args[-1] is not None:
            cmap = plot_kwargs.pop('cmap', None)
            colors = list(np.unique(plot_args[-1]))
            vmin, vmax = plot_kwargs.pop('vmin'), plot_kwargs.pop('vmax')
        else:
            cmap = None
            plot_args = plot_args[:-1]

        vectorized = {k: v for k, v in plot_kwargs.items() if isinstance(v, np.ndarray)}

        texts = []
        for i, item in enumerate(zip(*plot_args, strict=None)):
            x, y, text = item[:3]
            if len(item) == 4 and cmap is not None:
                color = item[3]
                if dtype_kind(plot_args[-1]) in 'if':
                    color = (color - vmin) / (vmax-vmin)
                    plot_kwargs['color'] = cmap(color)
                else:
                    color = colors.index(color) if color in colors else np.nan
                    plot_kwargs['color'] = cmap(color)
            kwargs = dict(plot_kwargs, **{k: v[i] for k, v in vectorized.items()})
            texts.append(ax.text(x, y, text, **kwargs))
        return {'artist': texts}

    def teardown_handles(self):
        if 'artist' in self.handles:
            for artist in self.handles['artist']:
                artist.remove()


class ArrowPlot(AnnotationPlot):
    """Draw an arrow using the information supplied to the Arrow annotation

    """

    _arrow_style_opts = ['alpha', 'color', 'lw', 'linewidth', 'visible']
    _text_style_opts = [*TextPlot.style_opts, 'textsize', 'fontsize']

    style_opts = sorted(set(_arrow_style_opts + _text_style_opts))

    def draw_annotation(self, axis, data, opts):
        x, y, text, direction, points, arrowstyle = data
        if self.invert_axes: x, y = y, x
        direction = direction.lower()
        arrowprops = dict({'arrowstyle':arrowstyle},
                          **{k: opts[k] for k in self._arrow_style_opts if k in opts})
        textopts = {k: opts[k] for k in self._text_style_opts if k in opts}
        if direction in ['v', '^']:
            xytext = (0, points if direction=='v' else -points)
        elif direction in ['>', '<']:
            xytext = (points if direction=='<' else -points, 0)
        if 'fontsize' in textopts:
            self.param.warning('Arrow fontsize style option is deprecated, '
                               'use textsize option instead.')
        if 'textsize' in textopts:
            textopts['fontsize'] = textopts.pop('textsize')
        return [axis.annotate(text, xy=(x, y), textcoords='offset points',
                              xytext=xytext, ha="center", va="center",
                              arrowprops=arrowprops, **textopts)]



class SplinePlot(AnnotationPlot):
    """Draw the supplied Spline annotation (see Spline docstring)

    """

    style_opts = ['alpha', 'edgecolor', 'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, data, opts):
        verts, codes = data
        if not len(verts):
            return []
        patch = patches.PathPatch(mpl.path.Path(verts, codes),
                                  facecolor='none', **opts)
        axis.add_patch(patch)
        return [patch]



class _SyntheticAnnotationPlot(AnnotationPlot):

    apply_ranges = param.Boolean(default=True, doc="""
        Whether to include the annotation in axis range calculations.""")

    style_opts = ['alpha', 'color', 'facecolor', 'edgecolor',
                  'linewidth', 'linestyle', 'visible']

    def draw_annotation(self, axis, positions, opts):
        if isinstance(positions, np.ndarray):
            values = positions
        else:
            size = len(self.hmap.last.kdims)
            first_keys = list(positions)[:size]
            values = zip(*[positions[n] for n in first_keys], strict=None)
        fn = getattr(axis, self._methods[self.invert_axes])
        return [fn(*val, **opts) for val in values]

    def get_extents(self, element, ranges=None, range_type='combined', **kwargs):
        import pandas as pd
        extents = super().get_extents(element, ranges, range_type)
        if isinstance(element, HLines):
            extents = np.nan, extents[0], np.nan, extents[2]
        elif isinstance(element, VLines):
            extents = extents[0], np.nan, extents[2], np.nan
        elif isinstance(element, HSpans):
            extents = pd.array(extents)
            extents = np.nan, extents[:2].min(), np.nan, extents[2:].max()
        elif isinstance(element, VSpans):
            extents = pd.array(extents)
            extents = extents[:2].min(), np.nan, extents[2:].max(), np.nan
        return extents

    def initialize_plot(self, ranges=None):
        figure = super().initialize_plot(ranges=ranges)
        labels = "yx" if self.invert_axes else "xy"
        if isinstance(figure, mpl.axes.Axes):
            figure.set_xlabel(labels[0])
            figure.set_ylabel(labels[1])
        else:
            figure.axes[0].set_xlabel(labels[0])
            figure.axes[0].set_ylabel(labels[1])
        return figure


class HLinesAnnotationPlot(_SyntheticAnnotationPlot):
    _methods = ("axhline", "axvline")


class VLinesAnnotationPlot(_SyntheticAnnotationPlot):
    _methods = ("axvline", "axhline")


class HSpansAnnotationPlot(_SyntheticAnnotationPlot):
    _methods = ("axhspan", "axvspan")


class VSpansAnnotationPlot(_SyntheticAnnotationPlot):
    _methods = ("axvspan", "axhspan")
