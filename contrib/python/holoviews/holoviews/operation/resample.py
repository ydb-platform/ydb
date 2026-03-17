import numpy as np
import param
from param.parameterized import bothmethod

from ..core import Dataset, Operation
from ..core.util import datetime_types, dt_to_int, isfinite, max_range
from ..element import Image
from ..streams import PlotSize, RangeX, RangeXY


class LinkableOperation(Operation):
    """Abstract baseclass for operations supporting linked inputs.

    """

    link_inputs = param.Boolean(default=True, doc="""
        By default, the link_inputs parameter is set to True so that
        when applying an operation, backends that support linked
        streams update RangeXY streams on the inputs of the operation.
        Disable when you do not want the resulting plot to be
        interactive, e.g. when trying to display an interactive plot a
        second time.""")

    _allow_extra_keywords=True


class ResampleOperation1D(LinkableOperation):
    """Abstract baseclass for resampling operations

    """

    dynamic = param.Boolean(default=True, doc="""
       Enables dynamic processing by default.""")

    x_range = param.Tuple(default=None, length=2, doc="""
       The x_range as a tuple of min and max x-value. Auto-ranges
       if set to None.""")

    x_sampling = param.Number(default=None, doc="""
        Specifies the smallest allowed sampling interval along the x axis.""")

    streams = param.ClassSelector(default=[PlotSize, RangeX], class_=(dict, list), doc="""
       List or dictionary of streams that are applied if dynamic=True,
       allowing for dynamic interaction with the plot.""")

    width = param.Integer(default=400, doc="""
       The width of the output image in pixels.""")

    height = param.Integer(default=400, doc="""
       The height of the output image in pixels.""")

    pixel_ratio = param.Number(default=None, bounds=(0,None),
                               inclusive_bounds=(False,False), doc="""
       Pixel ratio applied to the height and width. Useful for higher
       resolution screens where the PlotSize stream reports 'nominal'
       dimensions in pixels that do not match the physical pixels. For
       instance, setting pixel_ratio=2 can give better results on Retina
       displays. Also useful for using lower resolution for speed.
       If not set explicitly, the zoom level of the browsers will be used,
       if available.""")


class ResampleOperation2D(ResampleOperation1D):
    """Abstract baseclass for resampling operations

    """

    dynamic = param.Boolean(default=True, doc="""
       Enables dynamic processing by default.""")

    expand = param.Boolean(default=True, doc="""
       Whether the x_range and y_range should be allowed to expand
       beyond the extent of the data.  Setting this value to True is
       useful for the case where you want to ensure a certain size of
       output grid, e.g. if you are doing masking or other arithmetic
       on the grids.  A value of False ensures that the grid is only
       just as large as it needs to be to contain the data, which will
       be faster and use less memory if the resulting aggregate is
       being overlaid on a much larger background.""")

    y_range = param.Tuple(default=None, length=2, doc="""
       The y-axis range as a tuple of min and max y value. Auto-ranges
       if set to None.""")

    y_sampling = param.Number(default=None, doc="""
        Specifies the smallest allowed sampling interval along the y axis.""")

    target = param.ClassSelector(class_=Dataset, doc="""
        A target Dataset which defines the desired x_range, y_range,
        width and height.
    """)

    streams = param.ClassSelector(default=[PlotSize, RangeXY], class_=(dict, list), doc="""
       List or dictionary of streams that are applied if dynamic=True,
       allowing for dynamic interaction with the plot.""")

    element_type = param.ClassSelector(class_=(Dataset,), instantiate=False,
                                        is_instance=False, default=Image,
                                        doc="""
        The type of the returned Elements, must be a 2D Dataset type.""")

    precompute = param.Boolean(default=False, doc="""
        Whether to apply precomputing operations. Precomputing can
        speed up resampling operations by avoiding unnecessary
        recomputation if the supplied element does not change between
        calls. The cost of enabling this option is that the memory
        used to represent this internal state is not freed between
        calls.""")

    _transfer_options = []

    @bothmethod
    def instance(self_or_cls,**params):
        filtered = {k:v for k,v in params.items() if k in self_or_cls.param}
        inst = super().instance(**filtered)
        inst._precomputed = {}
        return inst

    def _get_sampling(self, element, x, y, ndim=2, default=None):
        target = self.p.target
        if not isinstance(x, list) and x is not None:
            x = [x]
        if not isinstance(y, list) and y is not None:
            y = [y]

        if target:
            x0, y0, x1, y1 = target.bounds.lbrt()
            x_range, y_range = (x0, x1), (y0, y1)
            height, width = target.dimension_values(2, flat=False).shape
        else:
            if x is None:
                x_range = self.p.x_range or (-0.5, 0.5)
            elif self.p.expand or not self.p.x_range:
                if self.p.x_range and all(isfinite(v) for v in self.p.x_range):
                    x_range = self.p.x_range
                else:
                    x_range = max_range([element.range(xd) for xd in x])
            else:
                x0, x1 = self.p.x_range
                ex0, ex1 = max_range([element.range(xd) for xd in x])
                x_range = (np.nanmin([np.nanmax([x0, ex0]), ex1]),
                           np.nanmax([np.nanmin([x1, ex1]), ex0]))

            if (y is None and ndim == 2):
                y_range = self.p.y_range or default or (-0.5, 0.5)
            elif self.p.expand or not self.p.y_range:
                if self.p.y_range and all(isfinite(v) for v in self.p.y_range):
                    y_range = self.p.y_range
                elif default is None:
                    y_range = max_range([element.range(yd) for yd in y])
                else:
                    y_range = default
            else:
                y0, y1 = self.p.y_range
                if default is None:
                    ey0, ey1 = max_range([element.range(yd) for yd in y])
                else:
                    ey0, ey1 = default
                y_range = (np.nanmin([np.nanmax([y0, ey0]), ey1]),
                           np.nanmax([np.nanmin([y1, ey1]), ey0]))
            width, height = self.p.width, self.p.height
        (xstart, xend), (ystart, yend) = x_range, y_range

        xtype = 'numeric'
        if isinstance(xstart, str) or isinstance(xend, str):
            raise ValueError("Categorical data is not supported")
        elif isinstance(xstart, datetime_types) or isinstance(xend, datetime_types):
            xstart, xend = dt_to_int(xstart, 'ns'), dt_to_int(xend, 'ns')
            xtype = 'datetime'
        elif not np.isfinite(xstart) and not np.isfinite(xend):
            xstart, xend = 0, 0
            if x and element.get_dimension_type(x[0]) in datetime_types:
                xtype = 'datetime'

        ytype = 'numeric'
        if isinstance(ystart, str) or isinstance(yend, str):
            raise ValueError("Categorical data is not supported")
        elif isinstance(ystart, datetime_types) or isinstance(yend, datetime_types):
            ystart, yend = dt_to_int(ystart, 'ns'), dt_to_int(yend, 'ns')
            ytype = 'datetime'
        elif not np.isfinite(ystart) and not np.isfinite(yend):
            ystart, yend = 0, 0
            if y and element.get_dimension_type(y[0]) in datetime_types:
                ytype = 'datetime'

        # Adjust width and height depending on pixel ratio
        pixel_ratio = self._get_pixel_ratio()
        width = int(width * pixel_ratio)
        height = int(height * pixel_ratio)

        # Compute highest allowed sampling density
        xspan = xend - xstart
        yspan = yend - ystart
        if self.p.x_sampling:
            width = int(min([(xspan/self.p.x_sampling), width]))
        if self.p.y_sampling:
            height = int(min([(yspan/self.p.y_sampling), height]))
        if xstart == xend or width == 0:
            xunit, width = 0, 0
        else:
            xunit = float(xspan)/width
        if ystart == yend or height == 0:
            yunit, height = 0, 0
        else:
            yunit = float(yspan)/height

        xs, ys = (
            np.linspace(xstart+xunit/2., xend-xunit/2., width),
            np.linspace(ystart+yunit/2., yend-yunit/2., height)
        )
        return ((xstart, xend), (ystart, yend)), (xs, ys), (width, height), (xtype, ytype)

    def _get_pixel_ratio(self):
        if self.p.pixel_ratio is None:
            from panel import state
            if state.browser_info and isinstance(state.browser_info.device_pixel_ratio, (int, float)):
                return state.browser_info.device_pixel_ratio
            else:
                return 1
        else:
            return self.p.pixel_ratio

    def _dt_transform(self, x_range, y_range, xs, ys, xtype, ytype):
        (xstart, xend), (ystart, yend) = x_range, y_range
        if xtype == 'datetime':
            xstart, xend = np.array([xstart, xend]).astype('datetime64[ns]')
            xs = xs.astype('datetime64[ns]')
        if ytype == 'datetime':
            ystart, yend = np.array([ystart, yend]).astype('datetime64[ns]')
            ys = ys.astype('datetime64[ns]')
        return ((xstart, xend), (ystart, yend)), (xs, ys)
