import numpy as np

from ..core import Dataset, Dimension, util
from ..core.util import dtype_kind
from ..element import Bars, Graph
from ..element.util import categorical_aggregate2d
from .util import get_axis_padding


class GeomMixin:

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        """Use first two key dimensions to set names, and all four
        to set the data range.

        """
        kdims = element.kdims
        # loop over start and end points of segments
        # simultaneously in each dimension
        for kdim0, kdim1 in zip([kdims[i].label for i in range(2)],
                                [kdims[i].label for i in range(2,4)], strict=None):
            new_range = {}
            for kdim in [kdim0, kdim1]:
                # for good measure, update ranges for both start and end kdim
                for r in ranges[kdim]:
                    if r == 'factors':
                        new_range[r] = list(
                            util.unique_iterator(list(ranges[kdim0][r])+
                                                 list(ranges[kdim1][r]))
                        )
                    else:
                        # combine (x0, x1) and (y0, y1) in range calculation
                        new_range[r] = util.max_range([ranges[kd][r]
                                                       for kd in [kdim0, kdim1]])
            ranges[kdim0] = new_range
            ranges[kdim1] = new_range
        return super().get_extents(element, ranges, range_type)


class ChordMixin:

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        """A Chord plot is always drawn on a unit circle.

        """
        xdim, ydim = element.nodes.kdims[:2]
        if range_type not in ('combined', 'data', 'extents'):
            return xdim.range[0], ydim.range[0], xdim.range[1], ydim.range[1]
        no_labels = (element.nodes.get_dimension(self.label_index) is None and
                     self.labels is None)
        rng = 1.1 if no_labels else 1.4
        x0, x1 = util.max_range([xdim.range, (-rng, rng)])
        y0, y1 = util.max_range([ydim.range, (-rng, rng)])
        return (x0, y0, x1, y1)


class HeatMapMixin:

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        if range_type in ('data', 'combined'):
            agg = element.gridded
            xtype = agg.interface.dtype(agg, 0)
            shape = agg.interface.shape(agg, gridded=True)
            if xtype.kind in 'SUO':
                x0, x1 = (0-0.5, shape[1]-0.5)
            else:
                x0, x1 = element.range(0)
            ytype = agg.interface.dtype(agg, 1)
            if ytype.kind in 'SUO':
                y0, y1 = (-.5, shape[0]-0.5)
            else:
                y0, y1 = element.range(1)
            return (x0, y0, x1, y1)
        else:
            return super().get_extents(element, ranges, range_type)


class SpikesMixin:

    def _get_axis_dims(self, element):
        if 'spike_length' in self.lookup_options(element, 'plot').options:
            return  [element.dimensions()[0], None, None]
        return super()._get_axis_dims(element)

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        opts = self.lookup_options(element, 'plot').options
        if len(element.dimensions()) > 1 and 'spike_length' not in opts:
            ydim = element.get_dimension(1)
            s0, s1 = ranges[ydim.label]['soft']
            s0 = min(s0, 0) if util.isfinite(s0) else 0
            s1 = max(s1, 0) if util.isfinite(s1) else 0
            ranges[ydim.label]['soft'] = (s0, s1)
        proxy_dim = None
        if 'spike_length' in opts or len(element.dimensions()) == 1:
            proxy_dim = Dimension('proxy_dim')
            length = opts.get('spike_length', self.spike_length)
            if self.batched:
                bs, ts = [], []
                # Iterate over current NdOverlay and compute extents
                # from position and length plot options
                frame = self.current_frame or self.hmap.last
                for el in frame.values():
                    opts = self.lookup_options(el, 'plot').options
                    pos = opts.get('position', self.position)
                    bs.append(pos)
                    ts.append(pos+length)
                proxy_range = (np.nanmin(bs), np.nanmax(ts))
            else:
                proxy_range = (self.position, self.position+length)
            ranges['proxy_dim'] = {'data':    proxy_range,
                                  'hard':     (np.nan, np.nan),
                                  'soft':     proxy_range,
                                  'combined': proxy_range}
        return super().get_extents(element, ranges, range_type,
                                   ydim=proxy_dim)


class AreaMixin:

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        vdims = element.vdims[:2]
        vdim = vdims[0].label
        if len(vdims) > 1:
            new_range = {}
            for r in ranges[vdim]:
                if r != 'values':
                    new_range[r] = util.max_range([ranges[vd.label][r] for vd in vdims])
            ranges[vdim] = new_range
        else:
            s0, s1 = ranges[vdim]['soft']
            s0 = min(s0, 0) if util.isfinite(s0) else 0
            s1 = max(s1, 0) if util.isfinite(s1) else 0
            ranges[vdim]['soft'] = (s0, s1)
        return super().get_extents(element, ranges, range_type)


class BarsMixin:

    def _get_axis_dims(self, element):
        if element.ndims > 1 and not (self.stacked or not self.multi_level):
            xdims = element.kdims
        else:
            xdims = element.kdims[0]
        return (xdims, element.vdims[0])

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        """Make adjustments to plot extents by computing
        stacked bar heights, adjusting the bar baseline
        and forcing the x-axis to be categorical.

        """
        if self.batched:
            overlay = self.current_frame
            element = Bars(overlay.table(), kdims=element.kdims+overlay.kdims,
                           vdims=element.vdims)
            for kd in overlay.kdims:
                ranges[kd.label]['combined'] = overlay.range(kd)

        vdim = element.vdims[0].label
        s0, s1 = ranges[vdim]['soft']
        s0 = min(s0, 0) if util.isfinite(s0) else 0
        s1 = max(s1, 0) if util.isfinite(s1) else 0
        ranges[vdim]['soft'] = (s0, s1)
        l, b, r, t = super().get_extents(element, ranges, range_type, ydim=element.vdims[0])
        if range_type not in ('combined', 'data'):
            return l, b, r, t

        # Compute stack heights
        xdim = element.kdims[0]
        if self.stacked:
            ds = Dataset(element)
            pos_range = ds.select(**{vdim: (0, None)}).aggregate(xdim, function=np.sum).range(vdim)
            neg_range = ds.select(**{vdim: (None, 0)}).aggregate(xdim, function=np.sum).range(vdim)
            y0, y1 = util.max_range([pos_range, neg_range])
        else:
            y0, y1 = ranges[vdim]['combined']

        x0, x1 = (l, r) if (util.isnumeric(l) or isinstance(l, util.datetime_types)) else ('', '')
        if range_type == 'data':
            return (x0, y0, x1, y1)

        padding = 0 if self.overlaid else self.padding
        _, ypad, _ = get_axis_padding(padding)
        y0, y1 = util.dimension_range(y0, y1, ranges[vdim]['hard'], ranges[vdim]['soft'], ypad, self.logy)
        y0, y1 = util.dimension_range(y0, y1, self.ylim, (None, None))
        return (x0, y0, x1, y1)

    def _get_coords(self, element, ranges, as_string=True):
        """Get factors for categorical axes.

        """
        gdim = None
        sdim = None
        if element.ndims == 1:
            pass
        elif not self.stacked:
            gdim = element.get_dimension(1)
        else:
            sdim = element.get_dimension(1)

        xdim = element.dimensions()[0]

        xvals = None
        if xdim.values:
            xvals = xdim.values

        if gdim and not sdim:
            if not xvals and not gdim.values:
                xvals, gvals = categorical_aggregate2d._get_coords(element)
            else:
                if gdim.values:
                    gvals = gdim.values
                elif ranges.get(gdim.label, {}).get('factors') is not None:
                    gvals = ranges[gdim.label]['factors']
                else:
                    gvals = element.dimension_values(gdim, False)
                gvals = np.asarray(gvals)
                if xvals:
                    pass
                elif ranges.get(xdim.label, {}).get('factors') is not None:
                    xvals = ranges[xdim.label]['factors']
                else:
                    xvals = element.dimension_values(0, False)
                xvals = np.asarray(xvals)
            c_is_str = dtype_kind(xvals) in 'SU' or not as_string
            g_is_str = dtype_kind(gvals) in 'SU' or not as_string
            xvals = [x if c_is_str else xdim.pprint_value(x) for x in xvals]
            gvals = [g if g_is_str else gdim.pprint_value(g) for g in gvals]
            return xvals, gvals
        else:
            if xvals:
                pass
            elif ranges.get(xdim.label, {}).get('factors') is not None:
                xvals = ranges[xdim.label]['factors']
            else:
                xvals = element.dimension_values(0, False)
            xvals = np.asarray(xvals)
            c_is_str = dtype_kind(xvals) in 'SU' or not as_string
            xvals = [x if c_is_str else xdim.pprint_value(x) for x in xvals]
            return xvals, None


class MultiDistributionMixin:

    def _get_axis_dims(self, element):
        return element.kdims, element.vdims[0]

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        return super().get_extents(
            element, ranges, range_type, 'categorical', ydim=element.vdims[0]
        )

class GraphMixin:

    def _get_axis_dims(self, element):
        if isinstance(element, Graph):
            element = element.nodes
        return element.dimensions()[:2]

    def get_extents(self, element, ranges, range_type='combined', **kwargs):
        return super().get_extents(element.nodes, ranges, range_type)
