"""ChartData and related objects."""

from __future__ import annotations

import datetime
from collections.abc import Sequence
from numbers import Number

from pptx.chart.xlsx import (
    BubbleWorkbookWriter,
    CategoryWorkbookWriter,
    XyWorkbookWriter,
)
from pptx.chart.xmlwriter import ChartXmlWriter
from pptx.util import lazyproperty


class _BaseChartData(Sequence):
    """Base class providing common members for chart data objects.

    A chart data object serves as a proxy for the chart data table that will be written to an
    Excel worksheet; operating as a sequence of series as well as providing access to chart-level
    attributes. A chart data object is used as a parameter in :meth:`shapes.add_chart` and
    :meth:`Chart.replace_data`. The data structure varies between major chart categories such as
    category charts and XY charts.
    """

    def __init__(self, number_format="General"):
        super(_BaseChartData, self).__init__()
        self._number_format = number_format
        self._series = []

    def __getitem__(self, index):
        return self._series.__getitem__(index)

    def __len__(self):
        return self._series.__len__()

    def append(self, series):
        return self._series.append(series)

    def data_point_offset(self, series):
        """
        The total integer number of data points appearing in the series of
        this chart that are prior to *series* in this sequence.
        """
        count = 0
        for this_series in self:
            if series is this_series:
                return count
            count += len(this_series)
        raise ValueError("series not in chart data object")

    @property
    def number_format(self):
        """
        The formatting template string, e.g. '#,##0.0', that determines how
        X and Y values are formatted in this chart and in the Excel
        spreadsheet. A number format specified on a series will override this
        value for that series. Likewise, a distinct number format can be
        specified for a particular data point within a series.
        """
        return self._number_format

    def series_index(self, series):
        """
        Return the integer index of *series* in this sequence.
        """
        for idx, s in enumerate(self):
            if series is s:
                return idx
        raise ValueError("series not in chart data object")

    def series_name_ref(self, series):
        """
        Return the Excel worksheet reference to the cell containing the name
        for *series*.
        """
        return self._workbook_writer.series_name_ref(series)

    def x_values_ref(self, series):
        """
        The Excel worksheet reference to the X values for *series* (not
        including the column label).
        """
        return self._workbook_writer.x_values_ref(series)

    @property
    def xlsx_blob(self):
        """
        Return a blob containing an Excel workbook file populated with the
        contents of this chart data object.
        """
        return self._workbook_writer.xlsx_blob

    def xml_bytes(self, chart_type):
        """
        Return a blob containing the XML for a chart of *chart_type*
        containing the series in this chart data object, as bytes suitable
        for writing directly to a file.
        """
        return self._xml(chart_type).encode("utf-8")

    def y_values_ref(self, series):
        """
        The Excel worksheet reference to the Y values for *series* (not
        including the column label).
        """
        return self._workbook_writer.y_values_ref(series)

    @property
    def _workbook_writer(self):
        """
        The worksheet writer object to which layout and writing of the Excel
        worksheet for this chart will be delegated.
        """
        raise NotImplementedError("must be implemented by all subclasses")

    def _xml(self, chart_type):
        """
        Return (as unicode text) the XML for a chart of *chart_type*
        populated with the values in this chart data object. The XML is
        a complete XML document, including an XML declaration specifying
        UTF-8 encoding.
        """
        return ChartXmlWriter(chart_type, self).xml


class _BaseSeriesData(Sequence):
    """
    Base class providing common members for series data objects. A series
    data object serves as proxy for a series data column in the Excel
    worksheet. It operates as a sequence of data points, as well as providing
    access to series-level attributes like the series label.
    """

    def __init__(self, chart_data, name, number_format):
        self._chart_data = chart_data
        self._name = name
        self._number_format = number_format
        self._data_points = []

    def __getitem__(self, index):
        return self._data_points.__getitem__(index)

    def __len__(self):
        return self._data_points.__len__()

    def append(self, data_point):
        return self._data_points.append(data_point)

    @property
    def data_point_offset(self):
        """
        The integer count of data points that appear in all chart series
        prior to this one.
        """
        return self._chart_data.data_point_offset(self)

    @property
    def index(self):
        """
        Zero-based integer indicating the sequence position of this series in
        its chart. For example, the second of three series would return `1`.
        """
        return self._chart_data.series_index(self)

    @property
    def name(self):
        """
        The name of this series, e.g. 'Series 1'. This name is used as the
        column heading for the y-values of this series and may also appear in
        the chart legend and perhaps other chart locations.
        """
        return self._name if self._name is not None else ""

    @property
    def name_ref(self):
        """
        The Excel worksheet reference to the cell containing the name for
        this series.
        """
        return self._chart_data.series_name_ref(self)

    @property
    def number_format(self):
        """
        The formatting template string that determines how a number in this
        series is formatted, both in the chart and in the Excel spreadsheet;
        for example '#,##0.0'. If not specified for this series, it is
        inherited from the parent chart data object.
        """
        number_format = self._number_format
        if number_format is None:
            return self._chart_data.number_format
        return number_format

    @property
    def x_values(self):
        """
        A sequence containing the X value of each datapoint in this series,
        in data point order.
        """
        return [dp.x for dp in self._data_points]

    @property
    def x_values_ref(self):
        """
        The Excel worksheet reference to the X values for this chart (not
        including the column heading).
        """
        return self._chart_data.x_values_ref(self)

    @property
    def y_values(self):
        """
        A sequence containing the Y value of each datapoint in this series,
        in data point order.
        """
        return [dp.y for dp in self._data_points]

    @property
    def y_values_ref(self):
        """
        The Excel worksheet reference to the Y values for this chart (not
        including the column heading).
        """
        return self._chart_data.y_values_ref(self)


class _BaseDataPoint(object):
    """
    Base class providing common members for data point objects.
    """

    def __init__(self, series_data, number_format):
        super(_BaseDataPoint, self).__init__()
        self._series_data = series_data
        self._number_format = number_format

    @property
    def number_format(self):
        """
        The formatting template string that determines how the value of this
        data point is formatted, both in the chart and in the Excel
        spreadsheet; for example '#,##0.0'. If not specified for this data
        point, it is inherited from the parent series data object.
        """
        number_format = self._number_format
        if number_format is None:
            return self._series_data.number_format
        return number_format


class CategoryChartData(_BaseChartData):
    """
    Accumulates data specifying the categories and series values for a chart
    and acts as a proxy for the chart data table that will be written to an
    Excel worksheet. Used as a parameter in :meth:`shapes.add_chart` and
    :meth:`Chart.replace_data`.

    This object is suitable for use with category charts, i.e. all those
    having a discrete set of label values (categories) as the range of their
    independent variable (X-axis) values. Unlike the ChartData types for
    charts supporting a continuous range of independent variable values (such
    as XyChartData), CategoryChartData has a single collection of category
    (X) values and each data point in its series specifies only the Y value.
    The corresponding X value is inferred by its position in the sequence.
    """

    def add_category(self, label):
        """
        Return a newly created |data.Category| object having *label* and
        appended to the end of the category collection for this chart.
        *label* can be a string, a number, a datetime.date, or
        datetime.datetime object. All category labels in a chart must be the
        same type. All category labels in a chart having multi-level
        categories must be strings.
        """
        return self.categories.add_category(label)

    def add_series(self, name, values=(), number_format=None):
        """
        Add a series to this data set entitled *name* and having the data
        points specified by *values*, an iterable of numeric values.
        *number_format* specifies how the series values will be displayed,
        and may be a string, e.g. '#,##0' corresponding to an Excel number
        format.
        """
        series_data = CategorySeriesData(self, name, number_format)
        self.append(series_data)
        for value in values:
            series_data.add_data_point(value)
        return series_data

    @property
    def categories(self):
        """|data.Categories| object providing access to category-object hierarchy.

        Assigning an iterable of category labels (strings, numbers, or dates) replaces
        the |data.Categories| object with a new one containing a category for each label
        in the sequence.

        Creating a chart from chart data having date categories will cause the chart to
        have a |DateAxis| for its category axis.
        """
        if not getattr(self, "_categories", False):
            self._categories = Categories()
        return self._categories

    @categories.setter
    def categories(self, category_labels):
        categories = Categories()
        for label in category_labels:
            categories.add_category(label)
        self._categories = categories

    @property
    def categories_ref(self):
        """
        The Excel worksheet reference to the categories for this chart (not
        including the column heading).
        """
        return self._workbook_writer.categories_ref

    def values_ref(self, series):
        """
        The Excel worksheet reference to the values for *series* (not
        including the column heading).
        """
        return self._workbook_writer.values_ref(series)

    @lazyproperty
    def _workbook_writer(self):
        """
        The worksheet writer object to which layout and writing of the Excel
        worksheet for this chart will be delegated.
        """
        return CategoryWorkbookWriter(self)


class Categories(Sequence):
    """
    A sequence of |data.Category| objects, also having certain hierarchical
    graph behaviors for support of multi-level (nested) categories.
    """

    def __init__(self):
        super(Categories, self).__init__()
        self._categories = []
        self._number_format = None

    def __getitem__(self, idx):
        return self._categories.__getitem__(idx)

    def __len__(self):
        """
        Return the count of the highest level of category in this sequence.
        If it contains hierarchical (multi-level) categories, this number
        will differ from :attr:`category_count`, which is the number of leaf
        nodes.
        """
        return self._categories.__len__()

    def add_category(self, label):
        """
        Return a newly created |data.Category| object having *label* and
        appended to the end of this category sequence. *label* can be
        a string, a number, a datetime.date, or datetime.datetime object. All
        category labels in a chart must be the same type. All category labels
        in a chart having multi-level categories must be strings.

        Creating a chart from chart data having date categories will cause
        the chart to have a |DateAxis| for its category axis.
        """
        category = Category(label, self)
        self._categories.append(category)
        return category

    @property
    def are_dates(self):
        """
        Return |True| if the first category in this collection has a date
        label (as opposed to str or numeric). A date label is one of type
        datetime.date or datetime.datetime. Returns |False| otherwise,
        including when this category collection is empty. It also returns
        False when this category collection is hierarchical, because
        hierarchical categories can only be written as string labels.
        """
        if self.depth != 1:
            return False
        first_cat_label = self[0].label
        date_types = (datetime.date, datetime.datetime)
        if isinstance(first_cat_label, date_types):
            return True
        return False

    @property
    def are_numeric(self):
        """
        Return |True| if the first category in this collection has a numeric
        label (as opposed to a string label), including if that value is
        a datetime.date or datetime.datetime object (as those are converted
        to integers for storage in Excel). Returns |False| otherwise,
        including when this category collection is empty. It also returns
        False when this category collection is hierarchical, because
        hierarchical categories can only be written as string labels.
        """
        if self.depth != 1:
            return False
        # This method only tests the first category. The categories must
        # be of uniform type, and if they're not, there will be problems
        # later in the process, but it's not this method's job to validate
        # the caller's input.
        first_cat_label = self[0].label
        numeric_types = (Number, datetime.date, datetime.datetime)
        if isinstance(first_cat_label, numeric_types):
            return True
        return False

    @property
    def depth(self):
        """
        The number of hierarchy levels in this category graph. Returns 0 if
        it contains no categories.
        """
        categories = self._categories
        if not categories:
            return 0
        first_depth = categories[0].depth
        for category in categories[1:]:
            if category.depth != first_depth:
                raise ValueError("category depth not uniform")
        return first_depth

    def index(self, category):
        """
        The offset of *category* in the overall sequence of leaf categories.
        A non-leaf category gets the index of its first sub-category.
        """
        index = 0
        for this_category in self._categories:
            if category is this_category:
                return index
            index += this_category.leaf_count
        raise ValueError("category not in top-level categories")

    @property
    def leaf_count(self):
        """
        The number of leaf-level categories in this hierarchy. The return
        value is the same as that of `len()` only when the hierarchy is
        single level.
        """
        return sum(c.leaf_count for c in self._categories)

    @property
    def levels(self):
        """
        A generator of (idx, label) sequences representing the category
        hierarchy from the bottom up. The first level contains all leaf
        categories, and each subsequent is the next level up.
        """

        def levels(categories):
            # yield all lower levels
            sub_categories = [sc for c in categories for sc in c.sub_categories]
            if sub_categories:
                for level in levels(sub_categories):
                    yield level
            # yield this level
            yield [(cat.idx, cat.label) for cat in categories]

        for level in levels(self):
            yield level

    @property
    def number_format(self):
        """
        Read/write. Return a string representing the number format used in
        Excel to format these category values, e.g. '0.0' or 'mm/dd/yyyy'.
        This string is only relevant when the categories are numeric or date
        type, although it returns 'General' without error when the categories
        are string labels. Assigning |None| causes the default number format
        to be used, based on the type of the category labels.
        """
        GENERAL = "General"

        # defined value takes precedence
        if self._number_format is not None:
            return self._number_format

        # multi-level (should) always be string labels
        # zero depth means empty in which case we can't tell anyway
        if self.depth != 1:
            return GENERAL

        # everything except dates gets 'General'
        first_cat_label = self[0].label
        if isinstance(first_cat_label, (datetime.date, datetime.datetime)):
            return r"yyyy\-mm\-dd"
        return GENERAL

    @number_format.setter
    def number_format(self, value):
        self._number_format = value


class Category(object):
    """
    A chart category, primarily having a label to be displayed on the
    category axis, but also able to be configured in a hierarchy for support
    of multi-level category charts.
    """

    def __init__(self, label, parent):
        super(Category, self).__init__()
        self._label = label
        self._parent = parent
        self._sub_categories = []

    def add_sub_category(self, label):
        """
        Return a newly created |data.Category| object having *label* and
        appended to the end of the sub-category sequence for this category.
        """
        category = Category(label, self)
        self._sub_categories.append(category)
        return category

    @property
    def depth(self):
        """
        The number of hierarchy levels rooted at this category node. Returns
        1 if this category has no sub-categories.
        """
        sub_categories = self._sub_categories
        if not sub_categories:
            return 1
        first_depth = sub_categories[0].depth
        for category in sub_categories[1:]:
            if category.depth != first_depth:
                raise ValueError("category depth not uniform")
        return first_depth + 1

    @property
    def idx(self):
        """
        The offset of this category in the overall sequence of leaf
        categories. A non-leaf category gets the index of its first
        sub-category.
        """
        return self._parent.index(self)

    def index(self, sub_category):
        """
        The offset of *sub_category* in the overall sequence of leaf
        categories.
        """
        index = self._parent.index(self)
        for this_sub_category in self._sub_categories:
            if sub_category is this_sub_category:
                return index
            index += this_sub_category.leaf_count
        raise ValueError("sub_category not in this category")

    @property
    def leaf_count(self):
        """
        The number of leaf category nodes under this category. Returns
        1 if this category has no sub-categories.
        """
        if not self._sub_categories:
            return 1
        return sum(category.leaf_count for category in self._sub_categories)

    @property
    def label(self):
        """
        The value that appears on the axis for this category. The label can
        be a string, a number, or a datetime.date or datetime.datetime
        object.
        """
        return self._label if self._label is not None else ""

    def numeric_str_val(self, date_1904=False):
        """
        The string representation of the numeric (or date) label of this
        category, suitable for use in the XML `c:pt` element for this
        category. The optional *date_1904* parameter specifies the epoch used
        for calculating Excel date numbers.
        """
        label = self._label
        if isinstance(label, (datetime.date, datetime.datetime)):
            return "%.1f" % self._excel_date_number(date_1904)
        return str(self._label)

    @property
    def sub_categories(self):
        """
        The sequence of child categories for this category.
        """
        return self._sub_categories

    def _excel_date_number(self, date_1904):
        """
        Return an integer representing the date label of this category as the
        number of days since January 1, 1900 (or 1904 if date_1904 is
        |True|).
        """
        date, label = datetime.date, self._label
        # -- get date from label in type-independent-ish way
        date_ = date(label.year, label.month, label.day)
        epoch = date(1904, 1, 1) if date_1904 else date(1899, 12, 31)
        delta = date_ - epoch
        excel_day_number = delta.days

        # -- adjust for Excel mistaking 1900 for a leap year --
        if not date_1904 and excel_day_number > 59:
            excel_day_number += 1

        return excel_day_number


class ChartData(CategoryChartData):
    """
    |ChartData| is simply an alias for |CategoryChartData| and may be removed
    in a future release. All new development should use |CategoryChartData|
    for creating or replacing the data in chart types other than XY and
    Bubble.
    """


class CategorySeriesData(_BaseSeriesData):
    """
    The data specific to a particular category chart series. It provides
    access to the series label, the series data points, and an optional
    number format to be applied to each data point not having a specified
    number format.
    """

    def add_data_point(self, value, number_format=None):
        """
        Return a CategoryDataPoint object newly created with value *value*,
        an optional *number_format*, and appended to this sequence.
        """
        data_point = CategoryDataPoint(self, value, number_format)
        self.append(data_point)
        return data_point

    @property
    def categories(self):
        """
        The |data.Categories| object that provides access to the category
        objects for this series.
        """
        return self._chart_data.categories

    @property
    def categories_ref(self):
        """
        The Excel worksheet reference to the categories for this chart (not
        including the column heading).
        """
        return self._chart_data.categories_ref

    @property
    def values(self):
        """
        A sequence containing the (Y) value of each datapoint in this series,
        in data point order.
        """
        return [dp.value for dp in self._data_points]

    @property
    def values_ref(self):
        """
        The Excel worksheet reference to the (Y) values for this series (not
        including the column heading).
        """
        return self._chart_data.values_ref(self)


class XyChartData(_BaseChartData):
    """
    A specialized ChartData object suitable for use with an XY (aka. scatter)
    chart. Unlike ChartData, it has no category sequence. Rather, each data
    point of each series specifies both an X and a Y value.
    """

    def add_series(self, name, number_format=None):
        """
        Return an |XySeriesData| object newly created and added at the end of
        this sequence, identified by *name* and values formatted with
        *number_format*.
        """
        series_data = XySeriesData(self, name, number_format)
        self.append(series_data)
        return series_data

    @lazyproperty
    def _workbook_writer(self):
        """
        The worksheet writer object to which layout and writing of the Excel
        worksheet for this chart will be delegated.
        """
        return XyWorkbookWriter(self)


class BubbleChartData(XyChartData):
    """
    A specialized ChartData object suitable for use with a bubble chart.
    A bubble chart is essentially an XY chart where the markers are scaled to
    provide a third quantitative dimension to the exhibit.
    """

    def add_series(self, name, number_format=None):
        """
        Return a |BubbleSeriesData| object newly created and added at the end
        of this sequence, and having series named *name* and values formatted
        with *number_format*.
        """
        series_data = BubbleSeriesData(self, name, number_format)
        self.append(series_data)
        return series_data

    def bubble_sizes_ref(self, series):
        """
        The Excel worksheet reference for the range containing the bubble
        sizes for *series*.
        """
        return self._workbook_writer.bubble_sizes_ref(series)

    @lazyproperty
    def _workbook_writer(self):
        """
        The worksheet writer object to which layout and writing of the Excel
        worksheet for this chart will be delegated.
        """
        return BubbleWorkbookWriter(self)


class XySeriesData(_BaseSeriesData):
    """
    The data specific to a particular XY chart series. It provides access to
    the series label, the series data points, and an optional number format
    to be applied to each data point not having a specified number format.

    The sequence of data points in an XY series is significant; lines are
    plotted following the sequence of points, even if that causes a line
    segment to "travel backward" (implying a multi-valued function). The data
    points are not automatically sorted into increasing order by X value.
    """

    def add_data_point(self, x, y, number_format=None):
        """
        Return an XyDataPoint object newly created with values *x* and *y*,
        and appended to this sequence.
        """
        data_point = XyDataPoint(self, x, y, number_format)
        self.append(data_point)
        return data_point


class BubbleSeriesData(XySeriesData):
    """
    The data specific to a particular Bubble chart series. It provides access
    to the series label, the series data points, and an optional number
    format to be applied to each data point not having a specified number
    format.

    The sequence of data points in a bubble chart series is maintained
    throughout the chart building process because a data point has no unique
    identifier and can only be retrieved by index.
    """

    def add_data_point(self, x, y, size, number_format=None):
        """
        Append a new BubbleDataPoint object having the values *x*, *y*, and
        *size*. The optional *number_format* is used to format the Y value.
        If not provided, the number format is inherited from the series data.
        """
        data_point = BubbleDataPoint(self, x, y, size, number_format)
        self.append(data_point)
        return data_point

    @property
    def bubble_sizes(self):
        """
        A sequence containing the bubble size for each datapoint in this
        series, in data point order.
        """
        return [dp.bubble_size for dp in self._data_points]

    @property
    def bubble_sizes_ref(self):
        """
        The Excel worksheet reference for the range containing the bubble
        sizes for this series.
        """
        return self._chart_data.bubble_sizes_ref(self)


class CategoryDataPoint(_BaseDataPoint):
    """
    A data point in a category chart series. Provides access to the value of
    the datapoint and the number format with which it should appear in the
    Excel file.
    """

    def __init__(self, series_data, value, number_format):
        super(CategoryDataPoint, self).__init__(series_data, number_format)
        self._value = value

    @property
    def value(self):
        """
        The (Y) value for this category data point.
        """
        return self._value


class XyDataPoint(_BaseDataPoint):
    """
    A data point in an XY chart series. Provides access to the x and y values
    of the datapoint.
    """

    def __init__(self, series_data, x, y, number_format):
        super(XyDataPoint, self).__init__(series_data, number_format)
        self._x = x
        self._y = y

    @property
    def x(self):
        """
        The X value for this XY data point.
        """
        return self._x

    @property
    def y(self):
        """
        The Y value for this XY data point.
        """
        return self._y


class BubbleDataPoint(XyDataPoint):
    """
    A data point in a bubble chart series. Provides access to the x, y, and
    size values of the datapoint.
    """

    def __init__(self, series_data, x, y, size, number_format):
        super(BubbleDataPoint, self).__init__(series_data, x, y, number_format)
        self._size = size

    @property
    def bubble_size(self):
        """
        The value representing the size of the bubble for this data point.
        """
        return self._size
