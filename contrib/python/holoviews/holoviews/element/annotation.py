from numbers import Number

import numpy as np
import param

from ..core import Dimension, Element, Element2D
from ..core.data import Dataset
from ..core.util import datetime_types


class VectorizedAnnotation(Dataset, Element2D):

    _auto_indexable_1d = False


class VLines(VectorizedAnnotation):

    kdims = param.List(default=[Dimension('x')], bounds=(1, 1))
    group = param.String(default='VLines', constant=True)


class HLines(VectorizedAnnotation):

    kdims = param.List(default=[Dimension('y')], bounds=(1, 1))
    group = param.String(default='HLines', constant=True)


class HSpans(VectorizedAnnotation):

    kdims = param.List(default=[Dimension('y0'), Dimension('y1')],
                       bounds=(2, 2))

    group = param.String(default='HSpans', constant=True)


class VSpans(VectorizedAnnotation):

    kdims = param.List(default=[Dimension('x0'), Dimension('x1')],
                       bounds=(2, 2))

    group = param.String(default='VSpans', constant=True)



class Annotation(Element2D):
    """An Annotation is a special type of element that is designed to be
    overlaid on top of any arbitrary 2D element. Annotations have
    neither key nor value dimensions allowing them to be overlaid over
    any type of data.

    Note that one or more Annotations *can* be displayed without being
    overlaid on top of any other data. In such instances (by default)
    they will be displayed using the unit axis limits (0.0-1.0 in both
    directions) unless an explicit 'extents' parameter is
    supplied. The extents of the bottom Annotation in the Overlay is
    used when multiple Annotations are displayed together.

    """

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2))

    group = param.String(default='Annotation', constant=True)

    _auxiliary_component = True

    def __init__(self, data, **params):
        super().__init__(data, **params)

    def __len__(self):
        return 1

    def __getitem__(self, key):
        if key in self.dimensions():
            return self.dimension_values(key)
        if not isinstance(key, tuple) or len(key) == 1:
            key = (key, slice(None))
        elif len(key) == 0:
            return self.clone()
        if not all(isinstance(k, slice) for k in key):
            raise KeyError(f"{self.__class__.__name__} only support slice indexing")
        xkey, ykey = tuple(key[:len(self.kdims)])
        xstart, xstop = xkey.start, xkey.stop
        ystart, ystop = ykey.start, ykey.stop
        return self.clone(self.data, extents=(xstart, ystart, xstop, ystop))


    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index == 0:
            return np.array([self.data if np.isscalar(self.data) else self.data[index]])
        elif index == 1:
            return [] if np.isscalar(self.data) else np.array([self.data[1]])
        else:
            return super().dimension_values(dimension)

    # Note: This version of clone is identical in path.BaseShape
    # Consider implementing a mix-in class if it is needed again.
    def clone(self, *args, **overrides):
        if len(args) == 1 and isinstance(args[0], tuple):
            args = args[0]
        # Apply name mangling for __ attribute
        pos_args = getattr(self, '_' + type(self).__name__ + '__pos_params', [])
        settings = {k: v for k, v in dict(self.param.values(), **overrides).items()
                    if k not in pos_args[:len(args)]}
        if 'id' not in settings:
            settings['id'] = self.id
        return self.__class__(*args, **settings)


class VLine(Annotation):
    """Vertical line annotation at the given position."""

    group = param.String(default='VLine', constant=True)

    x = param.ClassSelector(default=0, class_=(Number, datetime_types), doc="""
       The x-position of the VLine which make be numeric or a timestamp.""")

    __pos_params = ['x']

    def __init__(self, x, **params):
        if isinstance(x, np.ndarray) and x.size == 1:
            x = np.atleast_1d(x)[0]
        super().__init__(x, x=x, **params)

    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index == 0:
            return np.array([self.data])
        elif index == 1:
            return np.array([np.nan])
        else:
            return super().dimension_values(dimension)


class HLine(Annotation):
    """Horizontal line annotation at the given position.

    """

    group = param.String(default='HLine', constant=True)

    y = param.ClassSelector(default=0, class_=(Number, datetime_types), doc="""
       The y-position of the HLine which make be numeric or a timestamp.""")

    __pos_params = ['y']

    def __init__(self, y, **params):
        if isinstance(y, np.ndarray) and y.size == 1:
            y = np.atleast_1d(y)[0]
        super().__init__(y, y=y, **params)

    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index == 0:
            return np.array([np.nan])
        elif index == 1:
            return np.array([self.data])
        else:
            return super().dimension_values(dimension)


class Slope(Annotation):
    """A line drawn with arbitrary slope and y-intercept"""

    slope = param.Number(default=0)

    y_intercept = param.Number(default=0)

    __pos_params = ['slope', 'y_intercept']

    def __init__(self, slope, y_intercept, kdims=None, vdims=None, **params):
        super().__init__(
            (slope, y_intercept), slope=slope, y_intercept=y_intercept,
            kdims=kdims, vdims=vdims, **params)

    @classmethod
    def from_scatter(cls, element, **kwargs):
        """Returns a Slope element given an element of x/y-coordinates

        Computes the slope and y-intercept from an element containing
        x- and y-coordinates.

        Parameters
        ----------
        element
            Element to compute slope from
        kwargs
            Keyword arguments to pass to the Slope element

        Returns
        -------
        Slope element
        """
        x, y = (element.dimension_values(i) for i in range(2))
        par = np.polyfit(x, y, 1, full=True)
        gradient=par[0][0]
        y_intercept=par[0][1]
        return cls(gradient, y_intercept, **kwargs)



class VSpan(Annotation):
    """Vertical span annotation at the given position."""

    group = param.String(default='VSpan', constant=True)

    x1 = param.ClassSelector(default=0, class_=(Number, datetime_types), allow_None=True, doc="""
       The start x-position of the VSpan which must be numeric or a timestamp.""")

    x2 = param.ClassSelector(default=0, class_=(Number, datetime_types), allow_None=True, doc="""
       The end x-position of the VSpan which must be numeric or a timestamp.""")

    __pos_params = ['x1', 'x2']

    def __init__(self, x1=None, x2=None, **params):
        super().__init__([x1, x2], x1=x1, x2=x2, **params)

    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index == 0:
            return np.array(self.data)
        elif index == 1:
            return np.array([np.nan, np.nan])
        else:
            return super().dimension_values(dimension)


class HSpan(Annotation):
    """Horizontal span annotation at the given position.

    """

    group = param.String(default='HSpan', constant=True)

    y1 = param.ClassSelector(default=0, class_=(Number, datetime_types), allow_None=True, doc="""
       The start y-position of the VSpan which must be numeric or a timestamp.""")

    y2 = param.ClassSelector(default=0, class_=(Number, datetime_types), allow_None=True, doc="""
       The end y-position of the VSpan which must be numeric or a timestamp.""")

    __pos_params = ['y1', 'y2']

    def __init__(self, y1=None, y2=None, **params):
        super().__init__([y1, y2], y1=y1, y2=y2, **params)

    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index == 0:
            return np.array([np.nan, np.nan])
        elif index == 1:
            return np.array(self.data)
        else:
            return super().dimension_values(dimension)



class Spline(Annotation):
    """Draw a spline using the given handle coordinates and handle
    codes. The constructor accepts a tuple in format (coords, codes).

    Follows format of matplotlib spline definitions as used in
    matplotlib.path.Path with the following codes:

        Path.STOP     : 0

        Path.MOVETO   : 1

        Path.LINETO   : 2

        Path.CURVE3   : 3

        Path.CURVE4   : 4

        Path.CLOSEPLOY: 79

    """

    group = param.String(default='Spline', constant=True)

    def __init__(self, spline_points, **params):
        super().__init__(spline_points, **params)

    def clone(self, data=None, shared_data=True, new_type=None, *args, **overrides):
        """Clones the object, overriding data and parameters.

        Parameters
        ----------
        data
            New data replacing the existing data
        shared_data : bool, optional
            Whether to use existing data
        new_type : optional
            Type to cast object to
        *args: Additional arguments to pass to constructor
        **overrides: New keyword arguments to pass to constructor

        Returns
        -------
        Cloned Spline
        """
        return Element2D.clone(self, data, shared_data, new_type,
                               *args, **overrides)

    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index in [0, 1]:
            return np.array([point[index] for point in self.data[0]])
        else:
            return super().dimension_values(dimension)



class Arrow(Annotation):
    """Draw an arrow to the given xy position with optional text at
    distance 'points' away.

    The direction of the arrow may be specified as well as the arrow head style.
    """

    x = param.ClassSelector(default=0, class_=(Number, datetime_types), doc="""
       The x-position of the arrow which make be numeric or a timestamp.""")

    y = param.ClassSelector(default=0, class_=(Number, datetime_types), doc="""
       The y-position of the arrow which make be numeric or a timestamp.""")

    text = param.String(default='', doc="Text associated with the arrow.")

    direction = param.Selector(default='<',
                                     objects=['<', '^', '>', 'v'], doc="""
        The cardinal direction in which the arrow is pointing. Accepted
        arrow directions are ``'<'``, '^', ``'>'`` and 'v'.""")

    arrowstyle = param.Selector(default='->',
                                      objects=['-', '->', '-[', '-|>', '<->', '<|-|>'],
                                      doc="""
        The arrowstyle used to draw the arrow. Accepted arrow styles are
        '-', ``'->'``, '-[', ``'-|>'``, ``'<->'`` and ``'<|-|>'``""")

    points = param.Number(default=40, doc="Font size of arrow text (if any).")

    group = param.String(default='Arrow', constant=True)

    __pos_params = ['x', 'y', 'text', 'direction', 'points', 'arrowstyle']

    def __init__(self, x, y, text='', direction='<',
                 points=40, arrowstyle='->', **params):

        info = (x, y, text, direction, points, arrowstyle)
        super().__init__(info, x=x, y=y,
                         text=text, direction=direction,
                         points=points, arrowstyle=arrowstyle,
                         **params)

    def __setstate__(self, d):
        """Add compatibility for unpickling old Arrow types with different
        .data format.

        """
        super().__setstate__(d)
        if len(self.data) == 5:
            direction, text, (x, y), points, arrowstyle = self.data
            self.data = (x, y, text, direction, points, arrowstyle)

    def dimension_values(self, dimension, expanded=True, flat=True):
        index = self.get_dimension_index(dimension)
        if index == 0:
            return np.array([self.x])
        elif index == 1:
            return np.array([self.y])
        else:
            return super().dimension_values(dimension)



class Text(Annotation):
    """Draw a text annotation at the specified position with custom
    fontsize, alignment and rotation.

    """

    x = param.ClassSelector(default=0, class_=(Number, str, datetime_types), doc="""
       The x-position of the arrow which make be numeric or a timestamp.""")

    y = param.ClassSelector(default=0, class_=(Number, str, datetime_types), doc="""
       The y-position of the arrow which make be numeric or a timestamp.""")

    text = param.String(default='', doc="The text to be displayed.")

    fontsize = param.Number(default=12, doc="Font size of the text.")

    rotation = param.Number(default=0, doc="Text rotation angle in degrees.")

    halign = param.Selector(default='center',
                                  objects=['left', 'right', 'center'], doc="""
       The horizontal alignment position of the displayed text. Allowed values
       are 'left', 'right' and 'center'.""")

    valign = param.Selector(default='center',
                                  objects=['top', 'bottom', 'center'], doc="""
       The vertical alignment position of the displayed text. Allowed values
       are 'center', 'top' and 'bottom'.""")

    group = param.String(default='Text', constant=True)

    __pos_params = ['x', 'y', 'text', 'fontsize', 'halign', 'valign', 'rotation']

    def __init__(self, x, y, text, fontsize=12,
                 halign='center', valign='center', rotation=0, **params):
        info = (x, y, text, fontsize, halign, valign, rotation)
        super().__init__(info, x=x, y=y, text=text,
                         fontsize=fontsize, rotation=rotation,
                         halign=halign, valign=valign, **params)



class Div(Element):
    """The Div element represents a div DOM node in an HTML document defined
    as a string containing valid HTML.

    """

    group = param.String(default='Div', constant=True)

    def __init__(self, data, **params):
        if data is None:
            data = ''
        if not isinstance(data, str):
            raise ValueError("Div element html data must be a string "
                             f"type, found {type(data).__name__} type.")
        super().__init__(data, **params)



class Labels(Dataset, Element2D):
    """Labels represents a collection of text labels associated with 2D
    coordinates. Unlike the Text annotation, Labels is a Dataset type
    which allows drawing vectorized labels from tabular or gridded
    data.

    """

    kdims = param.List(default=[Dimension('x'), Dimension('y')],
                       bounds=(2, 2), constant=True, doc="""
        The label of the x- and y-dimension of the Labels element in form
        of a string or dimension object.""")

    group = param.String(default='Labels', constant=True)

    vdims = param.List(default=[Dimension('Label')], bounds=(1, None), doc="""
        Defines the value dimension corresponding to the label text.""")
