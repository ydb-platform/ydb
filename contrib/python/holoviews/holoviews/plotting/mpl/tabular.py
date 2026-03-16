from collections import defaultdict

import param
from matplotlib.font_manager import FontProperties
from matplotlib.table import Table as mpl_Table

from .element import ElementPlot
from .plot import mpl_rc_context


class TablePlot(ElementPlot):
    """A TablePlot can plot both TableViews and ViewMaps which display
    as either a single static table or as an animated table
    respectively.

    """

    border = param.Number(default=0.05, bounds=(0.0, 0.5), doc="""
        The fraction of the plot that should be empty around the
        edges.""")

    float_precision = param.Integer(default=3, doc="""
        The floating point precision to use when printing float
        numeric data types.""")

    max_value_len = param.Integer(default=20, doc="""
        The maximum allowable string length of a value shown in any
        table cell. Any strings longer than this length will be
        truncated.""")

    max_font_size = param.Integer(default=12, doc="""
        The largest allowable font size for the text in each table
        cell.""")

    max_rows = param.Integer(default=15, doc="""
        The maximum number of Table rows before the table is
        summarized.""")

    font_types = param.Dict(default={'heading': FontProperties(weight='bold',
                            family='DejaVu Sans')}, doc="""
        The font style used for heading labels used for emphasis.""")

    style_opts = ['alpha', 'sketch_params']

    # Disable axes handling for Table plots
    _has_axes = False

    def __init__(self, table, **params):
        super().__init__(table, **params)
        if not self.dynamic:
            self.cell_widths = self._format_table()
        else:
            self.cell_widths = None

    def _format_table(self):
        cell_widths = defaultdict(int)
        for key in self.keys:
            element = self._get_frame(key)
            if element is None:
                continue
            self._update_cell_widths(element, cell_widths)
        return cell_widths

    def _update_cell_widths(self, element, cell_widths):
        # Mapping from the cell coordinates to the dictionary key.
        summarize = element.rows > self.max_rows
        half_rows = self.max_rows//2
        rows = min([self.max_rows, element.rows])
        for row in range(rows):
            adjusted_row = row
            for col in range(element.cols):
                if summarize and row == half_rows:
                    cell_text = "..."
                else:
                    if summarize and row > half_rows:
                        adjusted_row = (element.rows - self.max_rows + row)
                    cell_text = element.pprint_cell(adjusted_row, col)
                    if len(cell_text) > self.max_value_len:
                        cell_text = cell_text[:(self.max_value_len-3)]+'...'
                cell_widths[col] = max(len(cell_text) + 2, cell_widths[col])

    def _cell_value(self, element, row, col):
        summarize = element.rows > self.max_rows
        half_rows = self.max_rows//2
        if summarize and row == half_rows:
            cell_text = "..."
        else:
            if summarize and row > half_rows:
                row = (element.rows - self.max_rows + row)
            cell_text = element.pprint_cell(row, col)
            if len(cell_text) > self.max_value_len:
                cell_text = cell_text[:(self.max_value_len-3)]+'...'
        return cell_text

    @mpl_rc_context
    def initialize_plot(self, ranges=None):

        # Render table
        axes = self.handles['axis']
        element = self.hmap.last
        table = self._render_table(element, axes)
        self.handles['artist'] = table

        # Add to axes
        axes.set_axis_off()
        axes.add_table(table)
        return self._finalize_axis(self.keys[-1], element=element)

    def _render_table(self, element, axes):
        if self.dynamic:
            cell_widths = defaultdict(int)
            self._update_cell_widths(element, cell_widths)
        else:
            cell_widths = self.cell_widths

        size_factor = (1.0 - 2*self.border)
        table = mpl_Table(axes, bbox=[self.border, self.border,
                                      size_factor, size_factor])
        total_width = sum(cell_widths.values())
        height = size_factor / element.rows

        summarize = element.rows > self.max_rows
        half_rows = self.max_rows/2
        rows = min([self.max_rows, element.rows])
        for row in range(rows):
            adjusted_row = row
            for col in range(element.cols):
                if summarize and row > half_rows:
                    adjusted_row = (element.rows - self.max_rows + row)
                cell_value = self._cell_value(element, row, col)
                cellfont = self.font_types.get(element.cell_type(adjusted_row,col), None)
                width = cell_widths[col] / float(total_width)
                font_kwargs = dict(fontproperties=cellfont) if cellfont else {}
                table.add_cell(row, col, width, height, text=cell_value,  loc='center',
                               **font_kwargs)
        table.set_fontsize(self.max_font_size)
        table.auto_set_font_size(True)
        return table

    def update_handles(self, key, axes, element, ranges, style):
        table = self._render_table(element, axes)
        self.handles['artist'].remove()
        axes.add_table(table)
        self.handles['artist'] = table
        return {}
