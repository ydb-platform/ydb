import param
from bokeh.models import Column
from bokeh.models.widgets import (
    DataTable,
    DateEditor,
    DateFormatter,
    IntEditor,
    NumberEditor,
    NumberFormatter,
    StringEditor,
    StringFormatter,
    TableColumn,
)

from ...core import Dataset, Dimension
from ...core.util import dimension_sanitizer, dtype_kind, isdatetime
from ...element import ItemTable
from ...streams import Buffer
from ..plot import GenericElementPlot
from .plot import BokehPlot
from .selection import TabularSelectionDisplay


class TablePlot(BokehPlot, GenericElementPlot):

    hooks = param.HookList(default=[], doc="""
        Optional list of hooks called when finalizing a plot. The
        hook is passed the plot object and the displayed element, and
        other plotting handles can be accessed via plot.handles.""")

    height = param.Number(default=300)

    selected = param.List(default=None, doc="""
        The current selection as a list of integers corresponding
        to the selected items.""")

    width = param.Number(default=400)

    selection_display = TabularSelectionDisplay()

    style_opts = ['row_headers', 'selectable', 'editable',
                  'sortable', 'fit_columns', 'scroll_to_selection',
                  'index_position', 'visible']

    _stream_data = True

    def __init__(self, element, plot=None, **params):
        super().__init__(element, **params)
        self.handles = {} if plot is None else self.handles['plot']
        element_ids = self.hmap.traverse(lambda x: id(x), [Dataset, ItemTable])
        self.static = len(set(element_ids)) == 1 and len(self.keys) == len(self.hmap)
        self.callbacks, self.source_streams = self._construct_callbacks()
        self.streaming = [s for s in self.streams if isinstance(s, Buffer)]
        self.static_source = False

    def get_data(self, element, ranges, style):
        return ({dimension_sanitizer(d.name): element.dimension_values(d)
                 for d in element.dimensions()}, {}, style)

    def initialize_plot(self, ranges=None, plot=None, plots=None, source=None):
        """Initializes a new plot object with the last available frame.

        """
        # Get element key and ranges for frame
        element = self.hmap.last
        key = self.keys[-1]
        self.current_frame = element
        self.current_key = key

        style = self.lookup_options(element, 'style')[self.cyclic_index]
        data, _, style = self.get_data(element, ranges, style)
        if source is None:
            source = self._init_datasource(data)
        self.handles['source'] = self.handles['cds'] = source
        self.handles['selected'] = source.selected
        if self.selected is not None:
            source.selected.indices = self.selected

        columns = self._get_columns(element, data)
        style['reorderable'] = False
        table = DataTable(source=source, columns=columns, height=self.height,
                          width=self.width, **style)
        self.handles['table'] = table
        self.handles['glyph_renderer'] = table
        self._execute_hooks(element)
        self.drawn = True

        title = self._get_title_div(self.keys[-1], '10pt')
        if title:
            plot = Column(title, table)
            self.handles['title'] = title
        else:
            plot = table
        self.handles['plot'] = plot

        for cb in self.callbacks:
            cb.initialize()
        return plot

    def _get_columns(self, element, data):
        columns = []
        for d in element.dimensions():
            col = dimension_sanitizer(d.name)
            kind = dtype_kind(data[col])
            if kind == 'i':
                formatter = NumberFormatter()
                editor = IntEditor()
            elif kind == 'f':
                formatter = NumberFormatter(format='0,0.0[00000]')
                editor = NumberEditor()
            elif isdatetime(data[col]):
                dimtype = element.get_dimension_type(col)
                dformat = Dimension.type_formatters.get(dimtype, '%Y-%m-%d %H:%M:%S')
                formatter = DateFormatter(format=dformat)
                editor = DateEditor()
            else:
                formatter = StringFormatter()
                editor = StringEditor()
            column = TableColumn(field=dimension_sanitizer(d.name), title=d.pprint_label,
                                 editor=editor, formatter=formatter)
            columns.append(column)
        return columns


    def update_frame(self, key, ranges=None, plot=None):
        """Updates an existing plot with data corresponding
        to the key.

        """
        element = self._get_frame(key)
        self.param.update(**self.lookup_options(element, 'plot').options)
        self._get_title_div(key, '12pt')

        # Cache frame object id to skip updating data if unchanged
        previous_id = self.handles.get('previous_id', None)
        current_id = element._plot_id
        source = self.handles['source']
        self.handles['previous_id'] = current_id
        self.static_source = (self.dynamic and (current_id == previous_id))
        if (element is None or (not self.dynamic and self.static) or
            (self.streaming and self.streaming[0].data is self.current_frame.data
             and not self.streaming[0]._triggering) or self.static_source):
            if self.static_source and hasattr(self, 'selected') and self.selected is not None:
                self._update_selected(source)
            return
        style = self.lookup_options(element, 'style')[self.cyclic_index]
        data, _, style = self.get_data(element, ranges, style)
        columns = self._get_columns(element, data)
        self.handles['table'].columns = columns
        self._update_datasource(source, data)
