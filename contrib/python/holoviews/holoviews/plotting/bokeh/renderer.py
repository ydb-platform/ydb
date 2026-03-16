import base64
import logging
from io import BytesIO

import bokeh
import param
from bokeh.document import Document
from bokeh.io import curdoc
from bokeh.models import Model
from bokeh.themes.theme import Theme
from panel.io.notebook import render_mimebundle
from panel.io.state import state
from param.parameterized import bothmethod

from ...core import HoloMap, Store
from ..plot import Plot
from ..renderer import HTML_TAGS, MIME_TYPES, Renderer
from .util import compute_plot_size

default_theme = Theme(json={
    'attrs': {
        'Title': {'text_color': 'black', 'text_font_size': '12pt'}
    }
})


class BokehRenderer(Renderer):

    backend = param.String(default='bokeh', doc="The backend name.")


    fig = param.Selector(default='auto', objects=['html', 'json', 'auto', 'png'], doc="""
        Output render format for static figures. If None, no figure
        rendering will occur. """)

    holomap = param.Selector(default='auto',
                                   objects=['widgets', 'scrubber',
                                            None, 'gif', 'auto'], doc="""
        Output render multi-frame (typically animated) format. If
        None, no multi-frame rendering will occur.""")

    theme = param.ClassSelector(default=default_theme, class_=(Theme, str),
                                allow_None=True, doc="""
       The applicable Bokeh Theme object (if any).""")

    webgl = param.Boolean(default=True, doc="""
        Whether to render plots with WebGL if available""")

    # Defines the valid output formats for each mode.
    mode_formats = {'fig': ['html', 'auto', 'png'],
                    'holomap': ['widgets', 'scrubber', 'gif', 'auto', None]}

    _loaded = False
    _render_with_panel = True

    @bothmethod
    def _save_prefix(self_or_cls, ext):
        """Hook to prefix content for instance JS when saving HTML

        """
        return

    @bothmethod
    def get_plot(self_or_cls, obj, doc=None, renderer=None, **kwargs):
        """Given a HoloViews Viewable return a corresponding plot instance.
        Allows supplying a document attach the plot to, useful when
        combining the bokeh model with another plot.

        """
        plot = super().get_plot(obj, doc, renderer, **kwargs)
        if plot.document is None:
            plot.document = Document() if self_or_cls.notebook_context else curdoc()
        if self_or_cls.theme:
            plot.document.theme = self_or_cls.theme
        return plot

    def _figure_data(self, plot, fmt, doc=None, as_script=False, **kwargs):
        """Given a plot instance, an output format and an optional bokeh
        document, return the corresponding data. If as_script is True,
        the content will be split in an HTML and a JS component.

        """
        model = plot.state
        if doc is None:
            doc = plot.document
        else:
            plot.document = doc

        for m in model.references():
            m._document = None

        doc.theme = self.theme
        doc.add_root(model)

        # Bokeh raises warnings about duplicate tools and empty subplots
        # but at the holoviews level these are not issues
        logger = logging.getLogger(bokeh.core.validation.check.__file__)
        logger.disabled = True

        data = None
        if fmt == 'gif':
            from bokeh.io.export import get_screenshot_as_png
            from bokeh.io.webdriver import webdriver_control

            if state.webdriver is None:
                webdriver = webdriver_control.create()
            else:
                webdriver = state.webdriver

            nframes = len(plot)
            frames = []
            for i in range(nframes):
                plot.update(i)
                img = get_screenshot_as_png(plot.state, driver=webdriver)
                frames.append(img)
            if state.webdriver is not None:
                webdriver.close()

            bio = BytesIO()
            duration = (1./self.fps)*1000
            frames[0].save(bio, format='GIF', append_images=frames[1:],
                           save_all=True, duration=duration, loop=0)
            bio.seek(0)
            data = bio.read()
        elif fmt == 'png':
            from bokeh.io.export import get_screenshot_as_png
            img = get_screenshot_as_png(plot.state, driver=state.webdriver)
            imgByteArr = BytesIO()
            img.save(imgByteArr, format='PNG')
            data = imgByteArr.getvalue()
        else:
            div = render_mimebundle(plot.state, doc, plot.comm)[0]['text/html']

        if as_script and fmt in ['png', 'gif']:
            b64 = base64.b64encode(data).decode("utf-8")
            (mime_type, tag) = MIME_TYPES[fmt], HTML_TAGS[fmt]
            src = HTML_TAGS['base64'].format(mime_type=mime_type, b64=b64)
            div = tag.format(src=src, mime_type=mime_type, css='')

        plot.document = doc
        if as_script or data is None:
            return div
        else:
            return data

    @classmethod
    def plot_options(cls, obj, percent_size):
        """Given a holoviews object and a percentage size, apply heuristics
        to compute a suitable figure size. For instance, scaling layouts
        and grids linearly can result in unwieldy figure sizes when there
        are a large number of elements. As ad hoc heuristics are used,
        this functionality is kept separate from the plotting classes
        themselves.

        Used by the IPython Notebook display hooks and the save
        utility. Note that this can be overridden explicitly per object
        using the fig_size and size plot options.

        """
        obj = obj.last if isinstance(obj, HoloMap) else obj
        plot = Store.registry[cls.backend].get(type(obj), None)
        if not hasattr(plot, 'width') or not hasattr(plot, 'height'):
            from .plot import BokehPlot
            plot = BokehPlot
        options = plot.lookup_options(obj, 'plot').options
        width = options.get('width', plot.width)
        height = options.get('height', plot.height)
        if width is not None:
            options['width'] = int(width)
        if height is not None:
            options['height'] = int(height)
        return dict(options)

    @bothmethod
    def get_size(self_or_cls, plot):
        """Return the display size associated with a plot before
        rendering to any particular format. Used to generate
        appropriate HTML display.

        Returns a tuple of (width, height) in pixels.

        """
        if isinstance(plot, Plot):
            plot = plot.state
        elif not isinstance(plot, Model):
            raise ValueError('Can only compute sizes for HoloViews '
                             'and bokeh plot objects.')
        return compute_plot_size(plot)

    @classmethod
    def load_nb(cls, inline=True):
        cls._loaded = True
