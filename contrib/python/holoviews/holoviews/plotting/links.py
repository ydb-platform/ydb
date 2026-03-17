import weakref
from collections import defaultdict

import param

from ..core.util import dimension_sanitizer


class Link(param.Parameterized):
    """A Link defines some connection between a source and target object
    in their visualization. It is quite similar to a Stream as it
    allows defining callbacks in response to some change or event on
    the source object, however, unlike a Stream, it does not transfer
    data and make it available to user defined subscribers. Instead
    a Link directly causes some action to occur on the target, for JS
    based backends this usually means that a corresponding JS callback
    will effect some change on the target in response to a change on
    the source.

    A Link must define a source object which is what triggers events,
    but must not define a target. It is also possible to define bi-
    directional links between the source and target object.

    """

    # Mapping from a source id to a Link instance
    registry = weakref.WeakKeyDictionary()

    # Mapping to define callbacks by backend and Link type.
    # e.g. Link._callbacks['bokeh'][Stream] = Callback
    _callbacks = defaultdict(dict)

    # Whether the link requires a target
    _requires_target = False

    def __init__(self, source, target=None, **params):
        if source is None:
            raise ValueError(f'{type(self).__name__} must define a source')
        if self._requires_target and target is None:
            raise ValueError(f'{type(self).__name__} must define a target.')

        # Source is stored as a weakref to allow it to be garbage collected
        self._source = None if source is None else weakref.ref(source)
        self._target = None if target is None else weakref.ref(target)
        super().__init__(**params)
        self.link()

    @classmethod
    def register_callback(cls, backend, callback):
        """Register a LinkCallback providing the implementation for
        the Link for a particular backend.

        """
        cls._callbacks[backend][cls] = callback

    @property
    def source(self):
        return self._source() if self._source else None

    @property
    def target(self):
        return self._target() if self._target else None

    def link(self):
        """Registers the Link

        """
        if self.source in self.registry:
            links = self.registry[self.source]
            params = {
                k: v for k, v in self.param.values().items() if k != 'name'}
            for link in links:
                link_params = {
                    k: v for k, v in link.param.values().items() if k != 'name'}
                if (type(link) is type(self) and link.source is self.source
                    and link.target is self.target and params == link_params):
                    return
            self.registry[self.source].append(self)
        else:
            self.registry[self.source] = [self]

    def unlink(self):
        """Unregisters the Link

        """
        links = self.registry.get(self.source)
        if self in links:
            links.pop(links.index(self))


class RangeToolLink(Link):
    """The RangeToolLink sets up a link between a RangeTool on the source
    plot and the axes on the target plot. It is useful for exploring
    a subset of a larger dataset in more detail. By default it will
    link along the x-axis but using the axes parameter both axes may
    be linked to the tool.

    Example of how to use RangeToolLink can be found here:
    https ://www.holoviews.org/gallery/demos/bokeh/timeseries_range_tool.html

    """

    axes = param.ListSelector(default=['x'], objects=['x', 'y'], doc="""
        Which axes to link the tool to.""")

    boundsx = param.Tuple(default=None, length=2, doc="""
        (start, end) bounds for the x-axis""")

    boundsy = param.Tuple(default=None, length=2, doc="""
        (start, end) bounds for the y-axis""")

    intervalsx = param.Tuple(default=None, length=2, doc="""
        (min, max) intervals for the x-axis""")

    intervalsy = param.Tuple(default=None, length=2, doc="""
        (min, max) intervals for the y-axis""")

    use_handles = param.Boolean(default=True, doc="""
        Whether to display handles. Only available from Bokeh 3.5 onwards.""")

    start_gesture = param.Selector(default='tap', objects=['pan', 'tap', 'none'],
         doc="Gesture to start a range selection. Only available from Bokeh 3.5 onwards.")

    inverted = param.Boolean(default=True, doc="""
         Whether to invert the highlighting of the range selection.
         Only available from Bokeh 3.5 onwards.""")

    _requires_target = True

class DataLink(Link):
    """DataLink defines a link in the data between two objects allowing
    them to be selected together. In order for a DataLink to be
    established the source and target data must be of the same length.

    """

    _requires_target = True


class SelectionLink(Link):
    """Links the selection between two glyph renderers.

    """

    _requires_target = True


class VertexTableLink(Link):
    """Defines a Link between a Path type and a Table that will
    display the vertices of selected path.

    """

    vertex_columns = param.List(default=[])

    _requires_target = True

    def __init__(self, source, target, **params):
        if 'vertex_columns' not in params:
            dimensions = [dimension_sanitizer(d.name) for d in target.dimensions()[:2]]
            params['vertex_columns'] = dimensions
        super().__init__(source, target, **params)


class RectanglesTableLink(Link):
    """Links a Rectangles element to a Table.

    """

    _requires_target = True
