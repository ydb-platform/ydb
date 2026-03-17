"""Data point-related objects."""

from __future__ import annotations

from collections.abc import Sequence

from pptx.chart.datalabel import DataLabel
from pptx.chart.marker import Marker
from pptx.dml.chtfmt import ChartFormat
from pptx.util import lazyproperty


class _BasePoints(Sequence):
    """
    Sequence providing access to the individual data points in a series.
    """

    def __init__(self, ser):
        super(_BasePoints, self).__init__()
        self._element = ser
        self._ser = ser

    def __getitem__(self, idx):
        if idx < 0 or idx >= self.__len__():
            raise IndexError("point index out of range")
        return Point(self._ser, idx)


class BubblePoints(_BasePoints):
    """
    Sequence providing access to the individual data points in
    a |BubbleSeries| object.
    """

    def __len__(self):
        return min(
            self._ser.xVal_ptCount_val,
            self._ser.yVal_ptCount_val,
            self._ser.bubbleSize_ptCount_val,
        )


class CategoryPoints(_BasePoints):
    """
    Sequence providing access to individual |Point| objects, each
    representing the visual properties of a data point in the specified
    category series.
    """

    def __len__(self):
        return self._ser.cat_ptCount_val


class Point(object):
    """
    Provides access to the properties of an individual data point in
    a series, such as the visual properties of its marker and the text and
    font of its data label.
    """

    def __init__(self, ser, idx):
        super(Point, self).__init__()
        self._element = ser
        self._ser = ser
        self._idx = idx

    @lazyproperty
    def data_label(self):
        """
        The |DataLabel| object representing the label on this data point.
        """
        return DataLabel(self._ser, self._idx)

    @lazyproperty
    def format(self):
        """
        The |ChartFormat| object providing access to the shape formatting
        properties of this data point, such as line and fill.
        """
        dPt = self._ser.get_or_add_dPt_for_point(self._idx)
        return ChartFormat(dPt)

    @lazyproperty
    def marker(self):
        """
        The |Marker| instance for this point, providing access to the visual
        properties of the data point marker, such as fill and line. Setting
        these properties overrides any value set at the series level.
        """
        dPt = self._ser.get_or_add_dPt_for_point(self._idx)
        return Marker(dPt)


class XyPoints(_BasePoints):
    """
    Sequence providing access to the individual data points in an |XySeries|
    object.
    """

    def __len__(self):
        return min(self._ser.xVal_ptCount_val, self._ser.yVal_ptCount_val)
