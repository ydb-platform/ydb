"""Public API for all plotting renderers supported by HoloViews,
regardless of plotting package or backend.

"""
import base64
import os
from contextlib import contextmanager
from functools import partial
from io import BytesIO, StringIO

import panel as pn
import param
from bokeh.document import Document
from bokeh.embed import file_html
from bokeh.io import curdoc
from bokeh.resources import CDN, INLINE
from packaging.version import Version
from panel import config
from panel.io.notebook import (
    JupyterCommManagerBinary,
    ipywidget,
    load_notebook,
    render_mimebundle,
    render_model,
)
from panel.io.state import state
from panel.models.comm_manager import CommManager as PnCommManager
from panel.pane import HoloViews as HoloViewsPane
from panel.viewable import Viewable
from panel.widgets.player import PlayerBase
from param.parameterized import bothmethod
from pyviz_comms import CommManager

from ..core import AdjointLayout, DynamicMap, HoloMap, Layout
from ..core.data import disable_pipeline
from ..core.io import Exporter
from ..core.options import Compositor, SkipRendering, Store, StoreOptions
from ..core.util import unbound_dimensions
from ..streams import Stream
from . import Plot
from .util import collate, displayable, initialize_dynamic

PANEL_VERSION = Version(pn.__version__).release

# Tags used when visual output is to be embedded in HTML
IMAGE_TAG = "<img src='{src}' style='max-width:100%; margin: auto; display: block; {css}'/>"
VIDEO_TAG = """
<video controls style='max-width:100%; margin: auto; display: block; {css}'>
<source src='{src}' type='{mime_type}'>
Your browser does not support the video tag.
</video>"""
PDF_TAG = "<iframe src='{src}' style='width:100%; margin: auto; display: block; {css}'></iframe>"
HTML_TAG = "{src}"
INVALID_TAG = "<div>Cannot render {mime_type} in HTML</div>"

HTML_TAGS = {
    'base64': 'data:{mime_type};base64,{b64}', # Use to embed data
    'svg':  IMAGE_TAG,
    'png':  IMAGE_TAG,
    'gif':  IMAGE_TAG,
    'webm': VIDEO_TAG,
    'mp4':  VIDEO_TAG,
    'pdf':  PDF_TAG,
    'html': HTML_TAG,
    'pgf':  INVALID_TAG
}

MIME_TYPES = {
    'svg':  'image/svg+xml',
    'png':  'image/png',
    'gif':  'image/gif',
    'webm': 'video/webm',
    'mp4':  'video/mp4',
    'pdf':  'application/pdf',
    'pgf':  'text/pgf',
    'html': 'text/html',
    'json': 'text/json',
    'js':   'application/javascript',
    'jlab-hv-exec': 'application/vnd.holoviews_exec.v0+json',
    'jlab-hv-load': 'application/vnd.holoviews_load.v0+json',
    'server': None
}

static_template = """
<html>
  <body>
    {html}
  </body>
</html>
"""

class Renderer(Exporter):
    """The job of a Renderer is to turn the plotting state held within
    Plot classes into concrete, visual output in the form of the PNG,
    SVG, MP4 or WebM formats (among others). Note that a Renderer is a
    type of Exporter and must therefore follow the Exporter interface.

    The Renderer needs to be able to use the .state property of the
    appropriate Plot classes associated with that renderer in order to
    generate output. The process of 'drawing' is execute by the Plots
    and the Renderer turns the final plotting state into output.

    """

    center = param.Boolean(default=True, doc="""
        Whether to center the plot""")

    backend = param.String(doc="""
        The full, lowercase name of the rendering backend or third
        part plotting package used e.g. 'matplotlib' or 'cairo'.""")

    dpi = param.Integer(default=None, doc="""
        The render resolution in dpi (dots per inch)""")

    fig = param.Selector(default='auto', objects=['auto'], doc="""
        Output render format for static figures. If None, no figure
        rendering will occur. """)

    fps = param.Number(default=20, doc="""
        Rendered fps (frames per second) for animated formats.""")

    holomap = param.Selector(default='auto',
                                   objects=['scrubber','widgets', None, 'auto'], doc="""
        Output render multi-frame (typically animated) format. If
        None, no multi-frame rendering will occur.""")

    mode = param.Selector(default='default',
                                objects=['default', 'server'], doc="""
        Whether to render the object in regular or server mode. In server
        mode a bokeh Document will be returned which can be served as a
        bokeh server app. By default renders all output is rendered to HTML.""")

    size = param.Integer(default=100, doc="""
        The rendered size as a percentage size""")

    widget_location = param.Selector(default=None, allow_None=True, objects=[
        'left', 'bottom', 'right', 'top', 'top_left', 'top_right',
        'bottom_left', 'bottom_right', 'left_top', 'left_bottom',
        'right_top', 'right_bottom'], doc="""
        The position of the widgets relative to the plot.""")

    widget_mode = param.Selector(default='embed', objects=['embed', 'live'], doc="""
        The widget mode determining whether frames are embedded or generated
        'live' when interacting with the widget.""")

    css = param.Dict(default={}, doc="""
        Dictionary of CSS attributes and values to apply to HTML output.""")

    info_fn = param.Callable(default=None, allow_None=True, constant=True,  doc="""
        Renderers do not support the saving of object info metadata""")

    key_fn = param.Callable(default=None, allow_None=True, constant=True,  doc="""
        Renderers do not support the saving of object key metadata""")

    post_render_hooks = param.Dict(default={'svg':[], 'png':[]}, doc="""
       Optional dictionary of hooks that are applied to the rendered
       data (according to the output format) before it is returned.

       Each hook is passed the rendered data and the object that is
       being rendered. These hooks allow post-processing of rendered
       data before output is saved to file or displayed.""")

    # Defines the valid output formats for each mode.
    mode_formats = {'fig': [None, 'auto'],
                    'holomap': [None, 'auto']}

    # The comm_manager handles the creation and registering of client,
    # and server side comms
    comm_manager = CommManager

    # Define appropriate widget classes
    widgets = ['scrubber', 'widgets']

    # Whether in a notebook context, set when running Renderer.load_nb
    notebook_context = False

    # Plot registry
    _plots = {}

    # Whether to render plots with Panel
    _render_with_panel = False

    def __init__(self, **params):
        self.last_plot = None
        super().__init__(**params)

    def __call__(self, obj, fmt='auto', **kwargs):
        plot, fmt = self._validate(obj, fmt)
        info = {'file-ext': fmt, 'mime_type': MIME_TYPES[fmt]}

        if plot is None:
            return None, info
        elif self.mode == 'server':
            return self.server_doc(plot, doc=kwargs.get('doc')), info
        elif isinstance(plot, Viewable):
            return self.static_html(plot), info
        else:
            data = self._figure_data(plot, fmt, **kwargs)
            data = self._apply_post_render_hooks(data, obj, fmt)
            return data, info

    @bothmethod
    def get_plot(self_or_cls, obj, doc=None, renderer=None, comm=None, **kwargs):
        """Given a HoloViews Viewable return a corresponding plot instance.

        """
        if isinstance(obj, DynamicMap) and obj.unbounded:
            dims = ', '.join(f'{dim!r}' for dim in obj.unbounded)
            msg = ('DynamicMap cannot be displayed without explicit indexing '
                   'as {dims} dimension(s) are unbounded. '
                   '\nSet dimensions bounds with the DynamicMap redim.range '
                   'or redim.values methods.')
            raise SkipRendering(msg.format(dims=dims))

        # Initialize DynamicMaps with first data item
        initialize_dynamic(obj)

        if not renderer:
            renderer = self_or_cls
            if not isinstance(self_or_cls, Renderer):
                renderer = self_or_cls.instance()

        if not isinstance(obj, Plot):
            if not displayable(obj):
                obj = collate(obj)
                initialize_dynamic(obj)

            with disable_pipeline():
                obj = Compositor.map(obj, mode='data', backend=self_or_cls.backend)
            plot_opts = dict(self_or_cls.plot_options(obj, self_or_cls.size),
                             **kwargs)
            if isinstance(obj, AdjointLayout):
                obj = Layout(obj)
            plot = self_or_cls.plotting_class(obj)(obj, renderer=renderer,
                                                   **plot_opts)
            defaults = [kd.default for kd in plot.dimensions]
            init_key = tuple(v if d is None else d for v, d in
                             zip(plot.keys[0], defaults, strict=None))
            plot.update(init_key)
        else:
            plot = obj

        # Trigger streams which were marked as requiring an update
        triggers = []
        for p in plot.traverse():
            if not hasattr(p, '_trigger'):
                continue
            for trigger in p._trigger:
                if trigger not in triggers:
                    triggers.append(trigger)
            p._trigger = []
        for trigger in triggers:
            Stream.trigger([trigger])

        if isinstance(self_or_cls, Renderer):
            self_or_cls.last_plot = plot

        if comm:
            plot.comm = comm

        if comm or self_or_cls.mode == 'server':
            if doc is None:
                doc = Document() if self_or_cls.notebook_context else curdoc()
            plot.document = doc
        return plot

    @bothmethod
    def get_plot_state(self_or_cls, obj, renderer=None, **kwargs):
        """Given a HoloViews Viewable return a corresponding plot state.

        """
        if not isinstance(obj, Plot):
            obj = self_or_cls.get_plot(obj=obj, renderer=renderer, **kwargs)
        return obj.state

    def _validate(self, obj, fmt, **kwargs):
        """Helper method to be used in the __call__ method to get a
        suitable plot or widget object and the appropriate format.

        """
        if isinstance(obj, Viewable):
            return obj, 'html'

        fig_formats = self.mode_formats['fig']
        holomap_formats = self.mode_formats['holomap']

        holomaps = obj.traverse(lambda x: x, [HoloMap])
        dynamic = any(isinstance(m, DynamicMap) for m in holomaps)

        if fmt in ['auto', None]:
            if any(len(o) > 1 or (isinstance(o, DynamicMap) and
                                  unbound_dimensions(
                                      o.streams,
                                      o.kdims,
                                      no_duplicates=not o.positional_stream_args))
                   for o in holomaps):
                fmt = holomap_formats[0] if self.holomap in ['auto', None] else self.holomap
            else:
                fmt = fig_formats[0] if self.fig == 'auto' else self.fig

        if fmt in self.widgets:
            plot = self.get_widget(obj, fmt)
            fmt = 'html'
        elif dynamic or (self._render_with_panel and fmt == 'html'):
            plot = HoloViewsPane(obj, center=self.center, backend=self.backend,
                                 renderer=self)
        else:
            plot = self.get_plot(obj, renderer=self, **kwargs)

        all_formats = set(fig_formats + holomap_formats)
        if fmt not in all_formats:
            raise Exception(f"Format {fmt!r} not supported by mode {self.mode!r}. Allowed formats: {fig_formats + holomap_formats!r}")
        self.last_plot = plot
        return plot, fmt

    def _apply_post_render_hooks(self, data, obj, fmt):
        """Apply the post-render hooks to the data.

        """
        hooks = self.post_render_hooks.get(fmt,[])
        for hook in hooks:
            try:
                data = hook(data, obj)
            except Exception as e:
                self.param.warning(f"The post_render_hook {hook!r} could not "
                                   f"be applied:\n\n {e}")
        return data

    def html(self, obj, fmt=None, css=None, resources='CDN', **kwargs):
        """Renders plot or data structure and wraps the output in HTML.
        The comm argument defines whether the HTML output includes
        code to initialize a Comm, if the plot supplies one.

        """
        plot, fmt =  self._validate(obj, fmt)
        figdata, _ = self(plot, fmt, **kwargs)
        if isinstance(resources, str):
            resources = resources.lower()
        if css is None: css = self.css

        if isinstance(plot, Viewable):
            doc = Document()
            plot._render_model(doc)
            if resources == 'cdn':
                resources = CDN
            elif resources == 'inline':
                resources = INLINE
            return file_html(doc, resources)
        elif fmt in ['html', 'json']:
            return figdata
        elif fmt == 'svg':
            figdata = figdata.encode("utf-8")
        elif fmt == 'pdf' and 'height' not in css:
            _, h = self.get_size(plot)
            css['height'] = f"{int(h*self.dpi*1.15)}px"

        if isinstance(css, dict):
            css = '; '.join(f"{k}: {v}" for k, v in css.items())
        else:
            raise ValueError("CSS must be supplied as Python dictionary")

        b64 = base64.b64encode(figdata).decode("utf-8")
        (mime_type, tag) = MIME_TYPES[fmt], HTML_TAGS[fmt]
        src = HTML_TAGS['base64'].format(mime_type=mime_type, b64=b64)
        html = tag.format(src=src, mime_type=mime_type, css=css)
        return html

    def components(self, obj, fmt=None, comm=True, **kwargs):
        """Returns data and metadata dictionaries containing HTML and JS
        components to include render in app, notebook, or standalone
        document.

        """
        if isinstance(obj, Plot):
            plot = obj
        else:
            plot, fmt = self._validate(obj, fmt)

        if not isinstance(plot, Viewable):
            html = self._figure_data(plot, fmt, as_script=True, **kwargs)
            return {'text/html': html}, {MIME_TYPES['jlab-hv-exec']: {}}

        registry = list(Stream.registry.items())
        objects = plot.object.traverse(lambda x: x)
        dynamic, streams = False, False
        for source in objects:
            dynamic |= isinstance(source, DynamicMap)
            streams |= any(
                src is source or (src._plot_id is not None and src._plot_id == source._plot_id)
                for src, streams in registry for s in streams
            )
        if config.comms == 'colab':
            load_notebook(config.inline)
        embed = (not (dynamic or streams or self.widget_mode == 'live') or config.embed)
        if embed or config.comms == 'default':
            return self._render_panel(plot, embed, comm)
        return self._render_ipywidget(plot)

    def _render_panel(self, plot, embed=False, comm=True):
        comm = self.comm_manager.get_server_comm() if comm else None
        doc = Document()
        with config.set(embed=embed):
            model = plot.layout._render_model(doc, comm)
        if embed:
            return render_model(model, comm)
        ref = model.ref['id']
        manager = PnCommManager(comm_id=comm.id, plot_id=ref)
        client_comm = self.comm_manager.get_client_comm(
            on_msg=partial(plot._on_msg, ref, manager),
            on_error=partial(plot._on_error, ref),
            on_stdout=partial(plot._on_stdout, ref),
            on_open=lambda _: comm.init()
        )
        manager.client_comm_id = client_comm.id
        return render_mimebundle(model, doc, comm, manager)

    def _render_ipywidget(self, plot):
        # Handle rendering object as ipywidget
        widget = ipywidget(plot, combine_events=True)
        if hasattr(widget, '_repr_mimebundle_'):
            return widget._repr_mimebundle_(), {}
        plaintext = repr(widget)
        if len(plaintext) > 110:
            plaintext = plaintext[:110] + '…'
        data = {'text/plain': plaintext}
        if widget._view_name is not None:
            data['application/vnd.jupyter.widget-view+json'] = {
                'version_major': 2,
                'version_minor': 0,
                'model_id': widget._model_id
            }
        if config.comms == 'vscode':
            # Unfortunately VSCode does not yet handle _repr_mimebundle_
            from IPython.display import display
            display(data, raw=True)
            return {'text/html': '<div style="display: none"></div>'}, {}
        return data, {}

    def static_html(self, obj, fmt=None, template=None):
        """Generates a static HTML with the rendered object in the
        supplied format. Allows supplying a template formatting string
        with fields to interpolate 'js', 'css' and the main 'html'.

        """
        html_bytes = StringIO()
        self.save(obj, html_bytes, fmt)
        html_bytes.seek(0)
        return html_bytes.read()

    @bothmethod
    def get_widget(self_or_cls, plot, widget_type, **kwargs):
        if widget_type == 'scrubber':
            widget_location = self_or_cls.widget_location or 'bottom'
        else:
            widget_type = 'individual'
            widget_location = self_or_cls.widget_location or 'right'

        layout = HoloViewsPane(plot, widget_type=widget_type, center=self_or_cls.center,
                               widget_location=widget_location, renderer=self_or_cls)
        interval = int((1./self_or_cls.fps) * 1000)
        for player in layout.layout.select(PlayerBase):
            player.interval = interval
        return layout

    @bothmethod
    def export_widgets(self_or_cls, obj, filename, fmt=None, template=None,
                       json=False, json_path='', **kwargs):
        """Render and export object as a widget to a static HTML
        file. Allows supplying a custom template formatting string
        with fields to interpolate 'js', 'css' and the main 'html'
        containing the widget. Also provides options to export widget
        data to a json file in the supplied json_path (defaults to
        current path).

        """
        if fmt not in [*self_or_cls.widgets, "auto", None]:
            raise ValueError("Renderer.export_widget may only export "
                             "registered widget types.")
        self_or_cls.get_widget(obj, fmt).save(filename)

    @bothmethod
    def _widget_kwargs(self_or_cls):
        if self_or_cls.holomap in ('auto', 'widgets'):
            widget_type = 'individual'
            loc = self_or_cls.widget_location or 'right'
        else:
            widget_type = 'scrubber'
            loc = self_or_cls.widget_location or 'bottom'
        return {'widget_location': loc, 'widget_type': widget_type, 'center': True}

    @bothmethod
    def app(self_or_cls, plot, show=False, new_window=False, websocket_origin=None, port=0):
        """Creates a bokeh app from a HoloViews object or plot. By
        default simply attaches the plot to bokeh's curdoc and returns
        the Document, if show option is supplied creates an
        Application instance and displays it either in a browser
        window or inline if notebook extension has been loaded.  Using
        the new_window option the app may be displayed in a new
        browser tab once the notebook extension has been loaded.  A
        websocket origin is required when launching from an existing
        tornado server (such as the notebook) and it is not on the
        default port ('localhost:8888').

        """
        if isinstance(plot, HoloViewsPane):
            pane = plot
        else:
            pane = HoloViewsPane(plot, backend=self_or_cls.backend, renderer=self_or_cls,
                                 **self_or_cls._widget_kwargs())
        if new_window:
            return pane._get_server(port, websocket_origin, show=show)
        else:
            kwargs = {'notebook_url': websocket_origin} if websocket_origin else {}
            return pane.app(port=port, **kwargs)

    @bothmethod
    def server_doc(self_or_cls, obj, doc=None):
        """Get a bokeh Document with the plot attached. May supply
        an existing doc, otherwise bokeh.io.curdoc() is used to
        attach the plot to the global document instance.

        """
        if not isinstance(obj, HoloViewsPane):
            obj = HoloViewsPane(obj, renderer=self_or_cls, backend=self_or_cls.backend,
                                **self_or_cls._widget_kwargs())
        return obj.layout.server_doc(doc)

    @classmethod
    def plotting_class(cls, obj):
        """Given an object or Element class, return the suitable plotting
        class needed to render it with the current renderer.

        """
        if isinstance(obj, AdjointLayout) or obj is AdjointLayout:
            obj  = Layout
        if isinstance(obj, type):
            element_type = obj
        else:
            element_type = obj.type if isinstance(obj, HoloMap) else type(obj)
            if element_type is None:
                raise SkipRendering(f"{type(obj).__name__} was empty, could not determine plotting class.")
        try:
            plotclass = Store.registry[cls.backend][element_type]
        except KeyError:
            raise SkipRendering(f"No plotting class for {element_type.__name__} found.") from None
        return plotclass

    @classmethod
    def plot_options(cls, obj, percent_size):
        """Given an object and a percentage size (as supplied by the
        %output magic) return all the appropriate plot options that
        would be used to instantiate a plot class for that element.

        Default plot sizes at the plotting class level should be taken
        into account.

        """
        raise NotImplementedError

    @bothmethod
    def save(self_or_cls, obj, basename, fmt='auto', key=None, info=None,
             options=None, resources='inline', title=None, **kwargs):
        """Save a HoloViews object to file, either using an explicitly
        supplied format or to the appropriate default.

        """
        if info is None:
            info = {}
        if key is None:
            key = {}
        if info or key:
            raise Exception('Renderer does not support saving metadata to file.')

        if kwargs:
            param.main.param.warning("Supplying plot, style or norm options "
                               "as keyword arguments to the Renderer.save "
                               "method is deprecated and will error in "
                               "the next minor release.")

        with StoreOptions.options(obj, options, **kwargs):
            plot, fmt = self_or_cls._validate(obj, fmt)

        if isinstance(plot, Viewable):
            from bokeh.resources import CDN, INLINE, Resources
            if isinstance(resources, Resources):
                pass
            elif resources.lower() == 'cdn':
                resources = CDN
            elif resources.lower() == 'inline':
                resources = INLINE
            if isinstance(basename, str):
                if title is None:
                    title = os.path.basename(basename)
                if fmt in MIME_TYPES:
                    basename = f"{basename}.{fmt}"
            plot.layout.save(basename, embed=True, resources=resources, title=title)
            return

        rendered = self_or_cls(plot, fmt)
        if rendered is None: return
        (_data, info) = rendered
        encoded = self_or_cls.encode(rendered)
        prefix = self_or_cls._save_prefix(info['file-ext'])
        if prefix:
            encoded = prefix + encoded
        if isinstance(basename, (BytesIO, StringIO)):
            basename.write(encoded)
            basename.seek(0)
        else:
            filename =f"{basename}.{info['file-ext']}"
            with open(filename, 'wb') as f:
                f.write(encoded)

    @bothmethod
    def _save_prefix(self_or_cls, ext):
        """Hook to prefix content for instance JS when saving HTML

        """
        return

    @bothmethod
    def get_size(self_or_cls, plot):
        """Return the display size associated with a plot before
        rendering to any particular format. Used to generate
        appropriate HTML display.

        Returns a tuple of (width, height) in pixels.

        """
        raise NotImplementedError

    @classmethod
    @contextmanager
    def state(cls):
        """Context manager to handle global state for a backend,
        allowing Plot classes to temporarily override that state.

        """
        yield

    @classmethod
    def validate(cls, options):
        """Validate an options dictionary for the renderer.

        """
        return options

    @classmethod
    def load_nb(cls, inline=False, reloading=False, enable_mathjax=False):
        """Loads any resources required for display of plots
        in the Jupyter notebook

        """
        if PANEL_VERSION >= (1, 0, 2):
            load_notebook(inline, reloading=reloading, enable_mathjax=enable_mathjax)
        elif PANEL_VERSION >= (1, 0, 0):
            load_notebook(inline, reloading=reloading)
        elif reloading:
            return
        else:
            load_notebook(inline)
        with param.logging_level('ERROR'):
            try:
                ip = get_ipython() # noqa
            except Exception:
                ip = None
            if not ip or not hasattr(ip, 'kernel'):
                return
            cls.notebook_context = True
            cls.comm_manager = JupyterCommManagerBinary
            state._comm_manager = JupyterCommManagerBinary

    @classmethod
    def _delete_plot(cls, plot_id):
        """Deletes registered plots and calls Plot.cleanup

        """
        plot = cls._plots.get(plot_id)
        if plot is None:
            return
        plot.cleanup()
        del cls._plots[plot_id]
