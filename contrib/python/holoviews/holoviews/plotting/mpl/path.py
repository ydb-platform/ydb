import numpy as np
import param
from matplotlib.collections import LineCollection, PatchCollection
from matplotlib.dates import DateFormatter, date2num

from ...core import util
from ...core.dimension import Dimension
from ...core.options import abbreviated_exception
from ...core.util import dtype_kind
from ...element import Polygons
from ...util.transform import dim
from .element import ColorbarPlot
from .util import polygons_to_path_patches


class PathPlot(ColorbarPlot):

    aspect = param.Parameter(default='square', doc="""
        PathPlots axes usually define single space so aspect of Paths
        follows aspect in data coordinates by default.""")

    color_index = param.ClassSelector(default=None, class_=(str, int),
                                      allow_None=True, doc="""
      Index of the dimension from which the color will the drawn""")

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['alpha', 'color', 'linestyle', 'linewidth', 'visible', 'cmap']

    _collection = LineCollection

    def init_artists(self, ax, plot_args, plot_kwargs):
        if 'c' in plot_kwargs:
            plot_kwargs['array'] = plot_kwargs.pop('c')
        if 'vmin' in plot_kwargs and 'vmax' in plot_kwargs:
            plot_kwargs['clim'] = plot_kwargs.pop('vmin'), plot_kwargs.pop('vmax')
        if 'array' not in plot_kwargs and 'cmap' in plot_kwargs:
            del plot_kwargs['cmap']
        collection = self._collection(*plot_args, **plot_kwargs)
        ax.add_collection(collection)
        return {'artist': collection}

    def get_data(self, element, ranges, style):
        cdim = element.get_dimension(self.color_index)

        if cdim is None:
            color_style = style.get('color')
            if isinstance(color_style, str):
                cdim = element.get_dimension(color_style)
            elif isinstance(color_style, Dimension):
                cdim = element.get_dimension(color_style.label)
            elif isinstance(color_style, dim) and not color_style.ops:
                cdim = element.get_dimension(color_style.dimension.label)
            if cdim:
                style["color"] = cdim

        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        scalar = element.interface.isunique(element, cdim, per_geom=True) if cdim else False
        style_mapping = any(isinstance(v, util.arraylike_types) and not (k == 'c' and scalar)
                         for k, v in style.items())
        dims = element.kdims
        xdim, ydim = dims
        generic_dt_format = Dimension.type_formatters[np.datetime64]
        paths, cvals, dims = [], [], {}
        for path in element.split(datatype='columns'):
            xarr, yarr = path[xdim.name], path[ydim.name]
            if util.isdatetime(xarr):
                dt_format = Dimension.type_formatters.get(type(xarr[0]), generic_dt_format)
                xarr = date2num(xarr)
                dims[0] = xdim(value_format=DateFormatter(dt_format))
            if util.isdatetime(yarr):
                dt_format = Dimension.type_formatters.get(type(yarr[0]), generic_dt_format)
                yarr = date2num(yarr)
                dims[1] = ydim(value_format=DateFormatter(dt_format))
            arr = np.column_stack([xarr, yarr])
            # If neither color_index nor array-style mapping nor is present,
            # keep whole paths; otherwise, segment into (len(x)-1) segments for
            # correct mapping.
            if not (self.color_index is not None or style_mapping):
                paths.append(arr)
                continue
            length = len(xarr)
            for (s1, s2) in zip(range(length-1), range(1, length+1), strict=None):
                if cdim is not None:
                    pv = path[cdim.name]
                    if isinstance(pv, util.arraylike_types) and len(pv) == length:
                        # per-vertex values -> one value per segment (drop last)
                        cvals.append(pv[s1])
                    else:
                        # scalar per geometry -> repeat for each segment
                        cvals.append(pv)
                paths.append(arr[s1:s2+1])
        if self.invert_axes:
            paths = [p[::-1] for p in paths]
        if not (self.color_index or style_mapping or cdim):
            if cdim:
                style['array'] = style.pop('c')
                style['clim'] = style.pop('vmin', None), style.pop('vmax', None)
            return (paths,), style, {'dimensions': dims}
        if cdim:
            self._norm_kwargs(element, ranges, style, cdim)
            style['array'] = np.array(cvals)
            # When mapping color via array/cmap, drop scalar 'color' so it doesn't override
            style.pop('color', None)
        return (paths,), style, {'dimensions': dims}

    def update_handles(self, key, axis, element, ranges, style):
        artist = self.handles['artist']
        data, style, axis_kwargs = self.get_data(element, ranges, style)
        artist.set_paths(data[0])
        if 'array' in style:
            artist.set_array(style['array'])
        if 'vmin' in style and 'vmax' in style:
            artist.set_clim((style['vmin'], style['vmax']))
        if 'clim' in style:
            artist.set_clim(style['clim'])
        if 'norm' in style:
            artist.set_norm(style['norm'])
        artist.set_visible(style.get('visible', True))
        if 'colors' in style:
            artist.set_edgecolors(style['colors'])
        if 'facecolors' in style:
            artist.set_facecolors(style['facecolors'])
        if 'linewidth' in style:
            artist.set_linewidths(style['linewidth'])
        return axis_kwargs


class ContourPlot(PathPlot):

    color_index = param.ClassSelector(default=0, class_=(str, int),
                                      allow_None=True, doc="""
      Index of the dimension from which the color will the drawn""")

    def get_data(self, element, ranges, style):
        if isinstance(element, Polygons):
            color_prop = 'facecolors'
            subpaths = polygons_to_path_patches(element)
            paths = [path for subpath in subpaths for path in subpath]
            if self.invert_axes:
                for p in paths:
                    p._path.vertices = p._path.vertices[:, ::-1]
        else:
            color_prop = 'colors'
            paths = element.split(datatype='array', dimensions=element.kdims)
            if self.invert_axes:
                paths = [p[:, ::-1] for p in paths]

        # Process style transform
        with abbreviated_exception():
            style = self._apply_transforms(element, ranges, style)

        if 'c' in style:
            style['array'] = style.pop('c')
            style['clim'] = style.pop('vmin'), style.pop('vmax')
        elif isinstance(style.get('color'), np.ndarray):
            style[color_prop] = style.pop('color')

        # Process deprecated color_index
        if 'array' not in style:
            cidx = self.color_index+2 if isinstance(self.color_index, int) else self.color_index
            cdim = element.get_dimension(cidx)
        else:
            cdim = None

        if cdim is None:
            return (paths,), style, {}

        array = element.dimension_values(cdim, expanded=False)
        if len(paths) != len(array):
            # If there are multi-geometries the list of scalar values
            # will not match the list of paths and has to be expanded
            array = np.array([v for v, sps in zip(array, subpaths, strict=None)
                              for _ in range(len(sps))])

        if dtype_kind(array) not in 'uif':
            array = util.search_indices(array, util.unique_array(array))
        style['array'] = array
        self._norm_kwargs(element, ranges, style, cdim)
        return (paths,), style, {}


class PolygonPlot(ContourPlot):
    """PolygonPlot draws the polygon paths in the supplied Polygons
    object. If the Polygon has an associated value the color of
    Polygons will be drawn from the supplied cmap, otherwise the
    supplied facecolor will apply. Facecolor also determines the color
    for non-finite values.

    """

    show_legend = param.Boolean(default=False, doc="""
        Whether to show legend for the plot.""")

    style_opts = ['alpha', 'cmap', 'facecolor', 'edgecolor', 'linewidth',
                  'hatch', 'linestyle', 'joinstyle', 'fill', 'capstyle',
                  'color']

    _collection = PatchCollection
