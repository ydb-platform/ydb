import sys
from inspect import getmro

import param
from panel.layout import Row, Tabs
from panel.pane import PaneBase
from panel.util import param_name

from .core import DynamicMap, Element, HoloMap, Layout, Overlay, Store, ViewableElement
from .core.util import isscalar
from .element import Curve, Path, Points, Polygons, Rectangles, Table
from .plotting.links import (
    DataLink,
    RectanglesTableLink,
    SelectionLink,
    VertexTableLink,
)
from .streams import BoxEdit, CurveEdit, PointDraw, PolyDraw, PolyEdit, Selection1D


def preprocess(function, current=None):
    """Turns a param.depends watch call into a preprocessor method, i.e.
    skips all downstream events triggered by it.
    NOTE : This is a temporary hack while the addition of preprocessors
          in param is under discussion. This only works for the first
          method which depends on a particular parameter.
          (see https://github.com/pyviz/param/issues/332)

    """
    if current is None:
        current = []
    def inner(*args, **kwargs):
        self = args[0]
        self.param._BATCH_WATCH = True
        function(*args, **kwargs)
        self.param._BATCH_WATCH = False
        self.param._watchers = []
        self.param._events = []
    return inner


class annotate(param.ParameterizedFunction):
    """The annotate function allows drawing, editing and annotating any
    given Element (if it is supported). The annotate function returns
    a Layout of the editable plot and an Overlay of table(s), which
    allow editing the data of the element. The edited and annotated
    data may be accessed using the element and selected properties.

    """

    annotator = param.Parameter(doc="""The current Annotator instance.""")

    annotations = param.ClassSelector(default=[], class_=(dict, list), doc="""
        Annotations to associate with each object.""")

    edit_vertices = param.Boolean(default=True, doc="""
        Whether to add tool to edit vertices.""")

    empty_value = param.Parameter(default=None, doc="""
        The value to insert on annotation columns when drawing a new
        element.""")

    num_objects = param.Integer(default=None, bounds=(0, None), doc="""
        The maximum number of objects to draw.""")

    show_vertices = param.Boolean(default=True, doc="""
        Whether to show vertices when drawing the Path.""")

    table_transforms = param.HookList(default=[], doc="""
        Transform(s) to apply to element when converting data to Table.
        The functions should accept the Annotator and the transformed
        element as input.""")

    table_opts = param.Dict(default={'editable': True, 'width': 400}, doc="""
        Opts to apply to the editor table(s).""")

    vertex_annotations = param.ClassSelector(default=[], class_=(dict, list), doc="""
        Columns to annotate the Polygons with.""")

    vertex_style = param.Dict(default={'nonselection_alpha': 0.5}, doc="""
        Options to apply to vertices during drawing and editing.""")

    _annotator_types = {}

    @property
    def annotated(self):
        annotated = self.annotator.object
        if Store.current_backend == 'bokeh':
            return annotated.opts(clone=True, tools=['hover'])

    @property
    def selected(self):
        selected = self.annotator.selected
        if Store.current_backend == 'bokeh':
            return selected.opts(clone=True, tools=['hover'])

    @classmethod
    def compose(cls, *annotators):
        """Composes multiple annotator layouts and elements

        The composed Layout will contain all the elements in the
        supplied annotators and an overlay of all editor tables.

        Parameters
        ----------
        annotators
            Annotator layouts or elements to compose

        Returns
        -------
        A new layout consisting of the overlaid plots and tables
        """
        layers = []
        tables = []
        for annotator in annotators:
            if isinstance(annotator, Layout):
                l, ts = annotator
                layers.append(l)
                tables += list(ts)
            elif isinstance(annotator, annotate):
                layers.append(annotator.plot)
                tables += [t[0].object for t in annotator.editor]
            elif isinstance(annotator, (HoloMap, ViewableElement)):
                layers.append(annotator)
            else:
                raise ValueError(f"Cannot compose {type(annotator).__name__} type with annotators.")
        tables = Overlay(tables, group='Annotator')
        return (Overlay(layers).collate() + tables)

    def __call__(self, element, **params):
        overlay = element if isinstance(element, Overlay) else [element]

        layers = []
        annotator_type = None
        for el in overlay:
            matches = []
            for eltype, atype in self._annotator_types.items():
                if isinstance(el, eltype):
                    matches.append((getmro(type(el)).index(eltype), atype))
            if matches:
                if annotator_type is not None:
                    msg = ('An annotate call may only annotate a single element. '
                           'If you want to annotate multiple elements call annotate '
                           'on each one separately and then use the annotate.compose '
                           'method to combine them into a single layout.')
                    raise ValueError(msg)
                annotator_type = sorted(matches)[0][1]
                self.annotator = annotator_type(el, **params)
                tables = Overlay([t[0].object for t in self.annotator.editor], group='Annotator')
                layout = (self.annotator.plot + tables)
                layers.append(layout)
            else:
                layers.append(el)

        if annotator_type is None:
            obj = overlay if isinstance(overlay, Overlay) else element
            raise ValueError('Could not find an Element to annotate on'
                             f'{type(obj).__name__} object.')

        if len(layers) == 1:
            return layers[0]
        return self.compose(*layers)


class Annotator(PaneBase):
    """An Annotator allows drawing, editing and annotating a specific
    type of element. Each Annotator consists of the `plot` to draw and
    edit the element and the `editor`, which contains a list of tables,
    which make it possible to annotate each object in the element with
    additional properties defined in the `annotations`.

    """

    annotations = param.ClassSelector(default=[], class_=(dict, list), doc="""
        Annotations to associate with each object.""")

    default_opts = param.Dict(default={'responsive': True, 'min_height': 400,
                                       'padding': 0.1, 'framewise': True}, doc="""
        Opts to apply to the element.""")

    empty_value = param.Parameter(default=None, doc="""
        The value to insert on annotation columns when drawing a new
        element.""")

    object = param.ClassSelector(class_=Element, doc="""
        The Element to edit and annotate.""")

    num_objects = param.Integer(default=None, bounds=(0, None), doc="""
        The maximum number of objects to draw.""")

    table_transforms = param.HookList(default=[], doc="""
        Transform(s) to apply to element when converting data to Table.
        The functions should accept the Annotator and the transformed
        element as input.""")

    table_opts = param.Dict(default={'editable': True, 'width': 400}, doc="""
        Opts to apply to the editor table(s).""")

    # Once generic editing tools are merged into bokeh this could
    # include snapshot, restore and clear tools
    _tools = []

    # Allows patching on custom behavior
    _extra_opts = {}

    # Triggers for updates to the table
    _triggers = ['annotations', 'object', 'table_opts']

    # Links between plot and table
    _link_type = DataLink
    _selection_link_type = SelectionLink

    priority = 0.7

    @classmethod
    def applies(cls, obj):
        if 'holoviews' not in sys.modules:
            return False
        return isinstance(obj, cls.param.object.class_)

    @property
    def _element_type(self):
        return self.param.object.class_

    @property
    def _object_name(self):
        return self._element_type.__name__

    def __init__(self, object=None, **params):
        super().__init__(None, **params)
        self.object = self._process_element(object)
        self._table_row = Row()
        self.editor = Tabs((f'{param_name(self.name)}', self._table_row))
        self.plot = DynamicMap(self._get_plot)
        self.plot.callback.inputs[:] = [self.object]
        self._tables = []
        self._init_stream()
        self._stream.add_subscriber(self._update_object, precedence=0.1)
        self._selection = Selection1D(source=self.plot)
        self._update_table()
        self._update_links()
        self.param.watch(self._update, self._triggers)
        self.layout[:] = [self.plot, self.editor]

    @param.depends('annotations', 'object', 'default_opts')
    def _get_plot(self):
        return self._process_element(self.object)

    def _get_model(self, doc, root=None, parent=None, comm=None):
        return self.layout._get_model(doc, root, parent, comm)

    @preprocess
    def _update(self, event=None):
        if event and event.name == 'object':
            with param.discard_events(self):
                self.object = self._process_element(event.new)
        self._update_table()

    def _update_links(self):
        if hasattr(self, '_link'): self._link.unlink()
        self._link = self._link_type(self.plot, self._table)
        if self._selection_link_type:
            if hasattr(self, '_selection_link'): self._selection_link.unlink()
            self._selection_link = SelectionLink(self.plot, self._table)

    def _update_object(self, data=None):
        with param.discard_events(self):
            if len(self._stream.source) == 0:
                self.plot[()]
            self.object = self._stream.element

    def _update_table(self):
        object = self.object
        for transform in self.table_transforms:
            object = transform(object)
        self._table = Table(object, label=param_name(self.name)).opts(
            show_title=False, **self.table_opts)
        self._update_links()
        self._table_row[:] = [self._table]

    def select(self, selector=None):
        return self.layout.select(selector)

    @classmethod
    def compose(cls, *annotators):
        """Composes multiple Annotator instances and elements

        The composed Panel will contain all the elements in the
        supplied Annotators and Tabs containing all editors.

        Parameters
        ----------
        annotators
            Annotator objects or elements to compose

        Returns
        -------
        A new Panel consisting of the overlaid plots and tables
        """
        layers, tables = [], []
        for a in annotators:
            if isinstance(a, Annotator):
                layers.append(a.plot)
                tables += a.tables
            elif isinstance(a, Element):
                layers.append(a)
        return Row(Overlay(layers).collate(), Tabs(*tables))

    @property
    def tables(self):
        return list(zip(self.editor._names, self.editor, strict=None))

    @property
    def selected(self):
        return self.object.iloc[self._selection.index]



class PathAnnotator(Annotator):
    """Annotator which allows drawing and editing Paths and associating
    values with each path and each vertex of a path using a table.

    """

    edit_vertices = param.Boolean(default=True, doc="""
        Whether to add tool to edit vertices.""")

    object = param.ClassSelector(class_=Path, doc="""
        Path object to edit and annotate.""")

    show_vertices = param.Boolean(default=True, doc="""
        Whether to show vertices when drawing the Path.""")

    vertex_annotations = param.ClassSelector(default=[], class_=(dict, list), doc="""
        Columns to annotate the Polygons with.""")

    vertex_style = param.Dict(default={'nonselection_alpha': 0.5}, doc="""
        Options to apply to vertices during drawing and editing.""")

    _vertex_table_link = VertexTableLink

    _triggers = ['annotations', 'edit_vertices', 'object', 'table_opts',
                 'vertex_annotations']

    def __init__(self, object=None, **params):
        self._vertex_table_row = Row()
        super().__init__(object, **params)
        self.editor.append((f'{param_name(self.name)} Vertices',
                            self._vertex_table_row))

    def _init_stream(self):
        name = param_name(self.name)
        self._stream = PolyDraw(
            source=self.plot, data={}, num_objects=self.num_objects,
            show_vertices=self.show_vertices, tooltip=f'{name} Tool',
            vertex_style=self.vertex_style, empty_value=self.empty_value
        )
        if self.edit_vertices:
            self._vertex_stream = PolyEdit(
                source=self.plot, tooltip=f'{name} Edit Tool',
                vertex_style=self.vertex_style,
            )

    def _process_element(self, element=None):
        if element is None or not isinstance(element, self._element_type):
            datatype = list(self._element_type.datatype)
            datatype.remove('multitabular')
            datatype.append('multitabular')
            element = self._element_type(element, datatype=datatype)

        # Add annotation columns to poly data
        validate = []
        for col in self.annotations:
            if col in element:
                validate.append(col)
                continue
            init = self.annotations[col]() if isinstance(self.annotations, dict) else ''
            element = element.add_dimension(col, len(element.vdims), init, True)
        for col in self.vertex_annotations:
            if col in element:
                continue
            elif isinstance(self.vertex_annotations, dict):
                init = self.vertex_annotations[col]()
            else:
                init = ''
            element = element.add_dimension(col, len(element.vdims), init, True)

        # Validate annotations
        poly_data = {c: element.dimension_values(c, expanded=False)
                     for c in validate}
        if validate and len({len(v) for v in poly_data.values()}) != 1:
            raise ValueError('annotations must refer to value dimensions '
                             'which vary per path while at least one of '
                             f'{validate} varies by vertex.')

        # Add options to element
        tools = [tool() for tool in self._tools]
        opts = dict(tools=tools, color_index=None, **self.default_opts)
        opts.update(self._extra_opts)
        return element.options(**{k: v for k, v in opts.items()
                                  if k not in element.opts.get('plot').kwargs})

    def _update_links(self):
        super()._update_links()
        if hasattr(self, '_vertex_link'): self._vertex_link.unlink()
        self._vertex_link = self._vertex_table_link(self.plot, self._vertex_table)

    def _update_object(self, data=None):
        if self._stream.element is not None:
            element = self._stream.element
            if (element.interface.datatype == 'multitabular' and
                element.data and isinstance(element.data[0], dict)):
                for path in element.data:
                    for col in self.annotations:
                        if not isscalar(path[col]) and len(path[col]):
                            path[col] = path[col][0]
            with param.discard_events(self):
                self.object = element

    def _update_table(self):
        name = param_name(self.name)
        annotations = list(self.annotations)
        table = self.object
        for transform in self.table_transforms:
            table = transform(table)
        table_data = {a: list(table.dimension_values(a, expanded=False))
                      for a in annotations}
        self._table = Table(table_data, annotations, [], label=name).opts(
            show_title=False, **self.table_opts)
        self._vertex_table = Table(
            [], table.kdims, list(self.vertex_annotations), label=f'{name} Vertices'
        ).opts(show_title=False, **self.table_opts)
        self._update_links()
        self._table_row[:] = [self._table]
        self._vertex_table_row[:] = [self._vertex_table]

    @property
    def selected(self):
        index = self._selection.index
        data = [p for i, p in enumerate(self._stream.element.split()) if i in index]
        return self.object.clone(data)



class PolyAnnotator(PathAnnotator):
    """Annotator which allows drawing and editing Polygons and associating
    values with each polygon and each vertex of a Polygon using a table.

    """

    object = param.ClassSelector(class_=Polygons, doc="""
         Polygon element to edit and annotate.""")



class _GeomAnnotator(Annotator):

    default_opts = param.Dict(default={'responsive': True, 'min_height': 400,
                                       'padding': 0.1, 'framewise': True}, doc="""
        Opts to apply to the element.""")

    _stream_type = None

    __abstract = True

    def _init_stream(self):
        name = param_name(self.name)
        self._stream = self._stream_type(
            source=self.plot, data={}, num_objects=self.num_objects,
            tooltip=f'{name} Tool', empty_value=self.empty_value
        )

    def _process_element(self, object):
        if object is None or not isinstance(object, self._element_type):
            object = self._element_type(object)

        # Add annotations
        for col in self.annotations:
            if col in object:
                continue
            init = self.annotations[col]() if isinstance(self.annotations, dict) else ''
            object = object.add_dimension(col, len(object.vdims), init, True)

        # Add options
        tools = [tool() for tool in self._tools]
        opts = dict(tools=tools, **self.default_opts)
        opts.update(self._extra_opts)
        return object.options(**{k: v for k, v in opts.items()
                                 if k not in object.opts.get('plot').kwargs})



class PointAnnotator(_GeomAnnotator):
    """Annotator which allows drawing and editing Points and associating
    values with each point using a table.

    """

    default_opts = param.Dict(default={'responsive': True, 'min_height': 400,
                                       'padding': 0.1, 'size': 10,
                                       'framewise': True}, doc="""
        Opts to apply to the element.""")

    object = param.ClassSelector(class_=Points, doc="""
        Points element to edit and annotate.""")

    _stream_type = PointDraw


class CurveAnnotator(_GeomAnnotator):
    """Annotator which allows editing a Curve element and associating values
    with each vertex using a Table.

    """

    default_opts = param.Dict(default={'responsive': True, 'min_height': 400,
                                       'padding': 0.1, 'framewise': True}, doc="""
        Opts to apply to the element.""")

    object = param.ClassSelector(class_=Curve, doc="""
        Points element to edit and annotate.""")

    vertex_style = param.Dict(default={'size': 10}, doc="""
        Options to apply to vertices during drawing and editing.""")

    _stream_type = CurveEdit

    def _init_stream(self):
        name = param_name(self.name)
        self._stream = self._stream_type(
            source=self.plot, data={}, tooltip=f'{name} Tool',
            style=self.vertex_style
        )


class RectangleAnnotator(_GeomAnnotator):
    """Annotator which allows drawing and editing Rectangles and associating
    values with each point using a table.

    """

    object = param.ClassSelector(class_=Rectangles, doc="""
        Points element to edit and annotate.""")

    _stream_type = BoxEdit

    _link_type = RectanglesTableLink



# Register Annotators
annotate._annotator_types.update([
    (Polygons, PolyAnnotator),
    (Path, PathAnnotator),
    (Points, PointAnnotator),
    (Curve, CurveAnnotator),
    (Rectangles, RectangleAnnotator),
])
