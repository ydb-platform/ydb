import numpy as np
import param
from matplotlib import cm
from mpl_toolkits.mplot3d.art3d import Line3DCollection

from ...core import Dimension
from ...core.options import abbreviated_exception
from ...util.transform import dim as dim_expr
from ..util import map_colors
from .chart import PointPlot
from .element import ColorbarPlot
from .path import PathPlot
from .util import MPL_VERSION


class Plot3D(ColorbarPlot):
    """Plot3D provides a common baseclass for mplot3d based
    plots.

    """

    azimuth = param.Integer(default=-60, bounds=(-180, 180), doc="""
        Azimuth angle in the x,y plane.""")

    elevation = param.Integer(default=30, bounds=(0, 180), doc="""
        Elevation angle in the z-axis.""")

    distance = param.Integer(default=10, bounds=(7, 15), doc="""
        Distance from the plotted object.""")

    disable_axes = param.Boolean(default=False, doc="""
        Disable all axes.""")

    bgcolor = param.String(default='white', doc="""
        Background color of the axis.""")

    labelled = param.List(default=['x', 'y', 'z'], doc="""
        Whether to plot the 'x', 'y' and 'z' labels.""")

    projection = param.Selector(default='3d', objects=['3d'], doc="""
        The projection of the matplotlib axis.""")

    show_grid = param.Boolean(default=True, doc="""
        Whether to draw a grid in the figure.""")

    xaxis = param.Selector(default='fixed',
                                 objects=['fixed', None], doc="""
        Whether and where to display the xaxis.""")

    yaxis = param.Selector(default='fixed',
                                 objects=['fixed', None], doc="""
        Whether and where to display the yaxis.""")

    zaxis = param.Selector(default='fixed',
                                 objects=['fixed', None], doc="""
        Whether and where to display the yaxis.""")

    def _finalize_axis(self, key, **kwargs):
        """Extends the ElementPlot _finalize_axis method to set appropriate
        labels, and axes options for 3D Plots.

        """
        axis = self.handles['axis']
        self.handles['fig'].set_frameon(False)
        axis.grid(self.show_grid)
        axis.view_init(elev=self.elevation, azim=self.azimuth)
        try:
            axis._dist = self.distance
        except Exception:
            # axis.dist is deprecated see here:
            # https://github.com/matplotlib/matplotlib/pull/22084
            axis.dist = self.distance

        if self.xaxis is None:
            axis.w_xaxis.line.set_lw(0.)
            axis.w_xaxis.label.set_text('')
        if self.yaxis is None:
            axis.w_yaxis.line.set_lw(0.)
            axis.w_yaxis.label.set_text('')
        if self.zaxis is None:
            axis.w_zaxis.line.set_lw(0.)
            axis.w_zaxis.label.set_text('')
        if self.disable_axes:
            axis.set_axis_off()

        if MPL_VERSION <= (1, 5, 9):
            axis.set_axis_bgcolor(self.bgcolor)
        else:
            axis.set_facecolor(self.bgcolor)
        return super()._finalize_axis(key, **kwargs)

    def _draw_colorbar(self, element=None, dim=None, redraw=True):
        if element is None:
            element = self.hmap.last
        artist = self.handles.get('artist', None)

        fig = self.handles['fig']
        ax = self.handles['axis']
        # Get colorbar label
        if isinstance(dim, dim_expr):
            dim = dim.dimension
        if dim is None:
            if hasattr(self, 'color_index'):
                dim = element.get_dimension(self.color_index)
            else:
                dim = element.get_dimension(2)
        elif not isinstance(dim, Dimension):
            dim = element.get_dimension(dim)
        label = dim.pprint_label
        cbar = fig.colorbar(artist, shrink=0.7, ax=ax)
        self.handles['cbar'] = cbar
        self.handles['cax'] = cbar.ax
        self._adjust_cbar(cbar, label, dim)



class Scatter3DPlot(Plot3D, PointPlot):
    """Subclass of PointPlot allowing plotting of Points
    on a 3D axis, also allows mapping color and size
    onto a particular Dimension of the data.

    """

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
      Index of the dimension from which the color will the drawn""")

    size_index = param.ClassSelector(default=None, class_=(str, int),
                                     allow_None=True, doc="""
      Index of the dimension from which the sizes will the drawn.""")

    _plot_methods = dict(single='scatter')

    def get_data(self, element, ranges, style):
        xs, ys, zs = (element.dimension_values(i) for i in range(3))
        self._compute_styles(element, ranges, style)
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        if style.get('edgecolors') == 'none':
            style.pop('edgecolors')
        return (xs, ys, zs), style, {}

    def update_handles(self, key, axis, element, ranges, style):
        artist = self.handles['artist']
        artist._offsets3d, style, _ = self.get_data(element, ranges, style)
        cdim = element.get_dimension(self.color_index)
        if cdim and 'cmap' in style:
            clim = style['vmin'], style['vmax']
            cmap = cm.get_cmap(style['cmap'])
            artist._facecolor3d = map_colors(style['c'], clim, cmap, hex=False)
        if element.get_dimension(self.size_index):
            artist.set_sizes(style['s'])


class Path3DPlot(Plot3D, PathPlot):
    """Allows plotting paths on a 3D axis.

    """

    style_opts = ['alpha', 'color', 'linestyle', 'linewidth', 'visible', 'cmap']

    def get_data(self, element, ranges, style):
        paths = element.split(datatype='array', dimensions=element.kdims)
        if self.invert_axes:
            paths = [p[:, ::-1] for p in paths]

        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)
        if 'c' in style:
            style['array'] = style.pop('c')
        if isinstance(style.get('color'), np.ndarray):
            style['colors'] = style.pop('color')
        if 'vmin' in style:
            style['clim'] = style.pop('vmin', None), style.pop('vmax', None)
        return (paths,), style, {}

    def init_artists(self, ax, plot_args, plot_kwargs):
        line_segments = Line3DCollection(*plot_args, **plot_kwargs)
        ax.add_collection(line_segments)
        return {'artist': line_segments}

    def update_handles(self, key, axis, element, ranges, style):
        PathPlot.update_handles(self, key, axis, element, ranges, style)



class SurfacePlot(Plot3D):
    """Plots surfaces wireframes and contours in 3D space.
    Provides options to switch the display type via the
    plot_type parameter has support for a number of
    styling options including strides and colors.

    """

    colorbar = param.Boolean(default=False, doc="""
        Whether to add a colorbar to the plot.""")

    plot_type = param.Selector(default='surface',
                                     objects=['surface', 'wireframe',
                                              'contour'], doc="""
        Specifies the type of visualization for the Surface object.
        Valid values are 'surface', 'wireframe' and 'contour'.""")

    style_opts = ['antialiased', 'cmap', 'color', 'shade',
                  'linewidth', 'facecolors', 'rstride', 'cstride',
                  'norm', 'edgecolor', 'rcount', 'ccount']

    def init_artists(self, ax, plot_data, plot_kwargs):
        if self.plot_type == "wireframe":
            artist = ax.plot_wireframe(*plot_data, **plot_kwargs)
        elif self.plot_type == "surface":
            artist = ax.plot_surface(*plot_data, **plot_kwargs)
        elif self.plot_type == "contour":
            artist = ax.contour3D(*plot_data, **plot_kwargs)
        return {'artist': artist}

    def get_data(self, element, ranges, style):
        zdata = element.dimension_values(2, flat=False)
        data = np.ma.array(zdata, mask=np.logical_not(np.isfinite(zdata)))
        coords = [element.interface.coords(element, d, ordered=True,
                                           expanded=True)
                  for d in element.kdims]
        if self.invert_axes:
            coords = coords[::-1]
            data = data.T
        cmesh_data = [*coords, data]

        if self.plot_type != 'wireframe' and 'cmap' in style:
            self._norm_kwargs(element, ranges, style, element.vdims[0])
        return cmesh_data, style, {}



class TriSurfacePlot(Plot3D):
    """Plots a trisurface given a TriSurface element, containing
    X, Y and Z coordinates.

    """

    colorbar = param.Boolean(default=False, doc="""
        Whether to add a colorbar to the plot.""")

    style_opts = ['cmap', 'color', 'shade', 'linewidth', 'edgecolor',
                  'norm']

    _plot_methods = dict(single='plot_trisurf')

    def get_data(self, element, ranges, style):
        dims = element.dimensions()
        self._norm_kwargs(element, ranges, style, dims[2])
        x, y, z = (element.dimension_values(d) for d in dims)
        return (x, y, z), style, {}
