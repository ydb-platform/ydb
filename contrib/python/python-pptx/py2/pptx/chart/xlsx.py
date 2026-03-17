# encoding: utf-8

"""Chart builder and related objects."""

from contextlib import contextmanager

from xlsxwriter import Workbook

from ..compat import BytesIO


class _BaseWorkbookWriter(object):
    """Base class for workbook writers, providing shared members."""

    def __init__(self, chart_data):
        super(_BaseWorkbookWriter, self).__init__()
        self._chart_data = chart_data

    @property
    def xlsx_blob(self):
        """bytes for Excel file containing chart_data."""
        xlsx_file = BytesIO()
        with self._open_worksheet(xlsx_file) as (workbook, worksheet):
            self._populate_worksheet(workbook, worksheet)
        return xlsx_file.getvalue()

    @contextmanager
    def _open_worksheet(self, xlsx_file):
        """
        Enable XlsxWriter Worksheet object to be opened, operated on, and
        then automatically closed within a `with` statement. A filename or
        stream object (such as a ``BytesIO`` instance) is expected as
        *xlsx_file*.
        """
        workbook = Workbook(xlsx_file, {"in_memory": True})
        worksheet = workbook.add_worksheet()
        yield workbook, worksheet
        workbook.close()

    def _populate_worksheet(self, workbook, worksheet):
        """
        Must be overridden by each subclass to provide the particulars of
        writing the spreadsheet data.
        """
        raise NotImplementedError("must be provided by each subclass")


class CategoryWorkbookWriter(_BaseWorkbookWriter):
    """
    Determines Excel worksheet layout and can write an Excel workbook from
    a CategoryChartData object. Serves as the authority for Excel worksheet
    ranges.
    """

    @property
    def categories_ref(self):
        """
        The Excel worksheet reference to the categories for this chart (not
        including the column heading).
        """
        categories = self._chart_data.categories
        if categories.depth == 0:
            raise ValueError("chart data contains no categories")
        right_col = chr(ord("A") + categories.depth - 1)
        bottom_row = categories.leaf_count + 1
        return "Sheet1!$A$2:$%s$%d" % (right_col, bottom_row)

    def series_name_ref(self, series):
        """
        Return the Excel worksheet reference to the cell containing the name
        for *series*. This also serves as the column heading for the series
        values.
        """
        return "Sheet1!$%s$1" % self._series_col_letter(series)

    def values_ref(self, series):
        """
        The Excel worksheet reference to the values for this series (not
        including the column heading).
        """
        return "Sheet1!${col_letter}$2:${col_letter}${bottom_row}".format(
            **{
                "col_letter": self._series_col_letter(series),
                "bottom_row": len(series) + 1,
            }
        )

    @staticmethod
    def _column_reference(column_number):
        """Return str Excel column reference like 'BQ' for *column_number*.

        *column_number* is an int in the range 1-16384 inclusive, where
        1 maps to column 'A'.
        """
        if column_number < 1 or column_number > 16384:
            raise ValueError("column_number must be in range 1-16384")

        # ---Work right-to-left, one order of magnitude at a time. Note there
        #    is no zero representation in Excel address scheme, so this is
        #    not just a conversion to base-26---

        col_ref = ""
        while column_number:
            remainder = column_number % 26
            if remainder == 0:
                remainder = 26

            col_letter = chr(ord("A") + remainder - 1)
            col_ref = col_letter + col_ref

            # ---Advance to next order of magnitude or terminate loop. The
            # minus-one in this expression reflects the fact the next lower
            # order of magnitude has a minumum value of 1 (not zero). This is
            # essentially the complement to the "if it's 0 make it 26' step
            # above.---
            column_number = (column_number - 1) // 26

        return col_ref

    def _populate_worksheet(self, workbook, worksheet):
        """
        Write the chart data contents to *worksheet* in category chart
        layout. Write categories starting in the first column starting in
        the second row, and proceeding one column per category level (for
        charts having multi-level categories). Write series as columns
        starting in the next following column, placing the series title in
        the first cell.
        """
        self._write_categories(workbook, worksheet)
        self._write_series(workbook, worksheet)

    def _series_col_letter(self, series):
        """
        The letter of the Excel worksheet column in which the data for a
        series appears.
        """
        column_number = 1 + series.categories.depth + series.index
        return self._column_reference(column_number)

    def _write_categories(self, workbook, worksheet):
        """
        Write the categories column(s) to *worksheet*. Categories start in
        the first column starting in the second row, and proceeding one
        column per category level (for charts having multi-level categories).
        A date category is formatted as a date. All others are formatted
        `General`.
        """
        categories = self._chart_data.categories
        num_format = workbook.add_format({"num_format": categories.number_format})
        depth = categories.depth
        for idx, level in enumerate(categories.levels):
            col = depth - idx - 1
            self._write_cat_column(worksheet, col, level, num_format)

    def _write_cat_column(self, worksheet, col, level, num_format):
        """
        Write a category column defined by *level* to *worksheet* at offset
        *col* and formatted with *num_format*.
        """
        worksheet.set_column(col, col, 10)  # wide enough for a date
        for off, name in level:
            row = off + 1
            worksheet.write(row, col, name, num_format)

    def _write_series(self, workbook, worksheet):
        """
        Write the series column(s) to *worksheet*. Series start in the column
        following the last categories column, placing the series title in the
        first cell.
        """
        col_offset = self._chart_data.categories.depth
        for idx, series in enumerate(self._chart_data):
            num_format = workbook.add_format({"num_format": series.number_format})
            series_col = idx + col_offset
            worksheet.write(0, series_col, series.name)
            worksheet.write_column(1, series_col, series.values, num_format)


class XyWorkbookWriter(_BaseWorkbookWriter):
    """
    Determines Excel worksheet layout and can write an Excel workbook from XY
    chart data. Serves as the authority for Excel worksheet ranges.
    """

    def series_name_ref(self, series):
        """
        Return the Excel worksheet reference to the cell containing the name
        for *series*. This also serves as the column heading for the series
        Y values.
        """
        row = self.series_table_row_offset(series) + 1
        return "Sheet1!$B$%d" % row

    def series_table_row_offset(self, series):
        """
        Return the number of rows preceding the data table for *series* in
        the Excel worksheet.
        """
        title_and_spacer_rows = series.index * 2
        data_point_rows = series.data_point_offset
        return title_and_spacer_rows + data_point_rows

    def x_values_ref(self, series):
        """
        The Excel worksheet reference to the X values for this chart (not
        including the column label).
        """
        top_row = self.series_table_row_offset(series) + 2
        bottom_row = top_row + len(series) - 1
        return "Sheet1!$A$%d:$A$%d" % (top_row, bottom_row)

    def y_values_ref(self, series):
        """
        The Excel worksheet reference to the Y values for this chart (not
        including the column label).
        """
        top_row = self.series_table_row_offset(series) + 2
        bottom_row = top_row + len(series) - 1
        return "Sheet1!$B$%d:$B$%d" % (top_row, bottom_row)

    def _populate_worksheet(self, workbook, worksheet):
        """
        Write chart data contents to *worksheet* in the standard XY chart
        layout. Write the data for each series to a separate two-column
        table, X values in column A and Y values in column B. Place the
        series label in the first (heading) cell of the column.
        """
        chart_num_format = workbook.add_format(
            {"num_format": self._chart_data.number_format}
        )
        for series in self._chart_data:
            series_num_format = workbook.add_format(
                {"num_format": series.number_format}
            )
            offset = self.series_table_row_offset(series)
            # write X values
            worksheet.write_column(offset + 1, 0, series.x_values, chart_num_format)
            # write Y values
            worksheet.write(offset, 1, series.name)
            worksheet.write_column(offset + 1, 1, series.y_values, series_num_format)


class BubbleWorkbookWriter(XyWorkbookWriter):
    """
    Service object that knows how to write an Excel workbook from bubble
    chart data.
    """

    def bubble_sizes_ref(self, series):
        """
        The Excel worksheet reference to the range containing the bubble
        sizes for *series* (not including the column heading cell).
        """
        top_row = self.series_table_row_offset(series) + 2
        bottom_row = top_row + len(series) - 1
        return "Sheet1!$C$%d:$C$%d" % (top_row, bottom_row)

    def _populate_worksheet(self, workbook, worksheet):
        """
        Write chart data contents to *worksheet* in the bubble chart layout.
        Write the data for each series to a separate three-column table with
        X values in column A, Y values in column B, and bubble sizes in
        column C. Place the series label in the first (heading) cell of the
        values column.
        """
        chart_num_format = workbook.add_format(
            {"num_format": self._chart_data.number_format}
        )
        for series in self._chart_data:
            series_num_format = workbook.add_format(
                {"num_format": series.number_format}
            )
            offset = self.series_table_row_offset(series)
            # write X values
            worksheet.write_column(offset + 1, 0, series.x_values, chart_num_format)
            # write Y values
            worksheet.write(offset, 1, series.name)
            worksheet.write_column(offset + 1, 1, series.y_values, series_num_format)
            # write bubble sizes
            worksheet.write(offset, 2, "Size")
            worksheet.write_column(offset + 1, 2, series.bubble_sizes, chart_num_format)
