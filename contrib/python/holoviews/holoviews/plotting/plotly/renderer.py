import base64
from io import BytesIO

import panel as pn
import param
from param.parameterized import bothmethod

from ...core import HoloMap
from ...core.options import Store
from ..renderer import HTML_TAGS, MIME_TYPES, Renderer
from .callbacks import callbacks
from .util import (
    PLOTLY_GE_6_0_0,
    _convert_numpy_in_fig_dict,
    clean_internal_figure_properties,
)

with param.logging_level('CRITICAL'):
    import plotly.graph_objs as go


def _PlotlyHoloviewsPane(fig_dict, **kwargs):
    """Custom Plotly pane constructor for use by the HoloViews Pane.

    """
    # Remove internal HoloViews properties
    clean_internal_figure_properties(fig_dict)

    config = fig_dict.pop('config', {})
    if config.get('responsive'):
        kwargs['sizing_mode'] = 'stretch_both'
    plotly_pane = pn.pane.Plotly(fig_dict, viewport_update_policy='mouseup',
                                 config=config, **kwargs)

    # Register callbacks on pane
    for callback_cls in callbacks.values():
        for callback_prop in callback_cls.callback_properties:
            plotly_pane.param.watch(
                lambda event, cls=callback_cls, prop=callback_prop:
                    cls.update_streams_from_property_update(
                        prop, event.new, event.obj.object
                    ),
                callback_prop,
            )
    return plotly_pane


class PlotlyRenderer(Renderer):

    backend = param.String(default='plotly', doc="The backend name.")

    fig = param.Selector(default='auto', objects=['html', 'png', 'svg', 'auto'], doc="""
        Output render format for static figures. If None, no figure
        rendering will occur. """)

    holomap = param.Selector(default='auto',
                                   objects=['scrubber','widgets', 'gif',
                                            None, 'auto'], doc="""
        Output render multi-frame (typically animated) format. If
        None, no multi-frame rendering will occur.""")


    mode_formats = {'fig': ['html', 'png', 'svg'],
                    'holomap': ['widgets', 'scrubber', 'gif', 'auto']}

    widgets = ['scrubber', 'widgets']

    _loaded = False

    _render_with_panel = True

    @bothmethod
    def get_plot_state(self_or_cls, obj, doc=None, renderer=None, numpy_convert=False, **kwargs):
        """Given a HoloViews Viewable return a corresponding figure dictionary.
        Allows cleaning the dictionary of any internal properties that were added

        """
        fig_dict = super().get_plot_state(obj, renderer, **kwargs)
        config = fig_dict.get('config', {})

        # Remove internal properties (e.g. '_id', '_dim')
        clean_internal_figure_properties(fig_dict)

        # Run through Figure constructor to normalize keys
        # (e.g. to expand magic underscore notation)
        fig_dict = go.Figure(fig_dict).to_dict()
        fig_dict['config'] = config

        # Remove template
        fig_dict.get('layout', {}).pop('template', None)

        if numpy_convert and PLOTLY_GE_6_0_0:
            return _convert_numpy_in_fig_dict(fig_dict)

        return fig_dict

    def _figure_data(self, plot, fmt, as_script=False, **kwargs):
        if fmt == 'gif':
            import plotly.io as pio
            from PIL import Image
            from plotly.io.orca import ensure_server, shutdown_server, status

            running = status.state == 'running'
            if not running:
                ensure_server()

            nframes = len(plot)
            frames = []
            for i in range(nframes):
                plot.update(i)
                img_bytes = BytesIO()
                figure = go.Figure(self.get_plot_state(plot))
                img = pio.to_image(figure, 'png', validate=False)
                img_bytes.write(img)
                frames.append(Image.open(img_bytes))

            if not running:
                shutdown_server()

            bio = BytesIO()
            duration = (1./self.fps)*1000
            frames[0].save(bio, format='GIF', append_images=frames[1:],
                           save_all=True, duration=duration, loop=0)
            bio.seek(0)
            data = bio.read()
        elif fmt in ('png', 'svg'):
            import plotly.io as pio

            # Wrapping plot.state in go.Figure here performs validation
            # and applies any default theme.
            figure = go.Figure(self.get_plot_state(plot))
            data = pio.to_image(figure, fmt)

            if fmt == 'svg':
                data = data.decode('utf-8')
        else:
            raise ValueError(f"Unsupported format: {fmt}")

        if as_script:
            b64 = base64.b64encode(data).decode("utf-8")
            (mime_type, tag) = MIME_TYPES[fmt], HTML_TAGS[fmt]
            src = HTML_TAGS['base64'].format(mime_type=mime_type, b64=b64)
            div = tag.format(src=src, mime_type=mime_type, css='')
            return div
        return data


    @classmethod
    def plot_options(cls, obj, percent_size):
        factor = percent_size / 100.0
        obj = obj.last if isinstance(obj, HoloMap) else obj
        plot = Store.registry[cls.backend].get(type(obj), None)
        options = plot.lookup_options(obj, 'plot').options
        width = options.get('width', plot.width) * factor
        height = options.get('height', plot.height) * factor
        return dict(options, width=int(width), height=int(height))


    @classmethod
    def load_nb(cls, inline=True):
        """Loads the plotly notebook resources.

        """
        import panel.models.plotly # noqa
        cls._loaded = True
        if 'plotly' not in getattr(pn.extension, '_loaded_extensions', ['plotly']):
            pn.extension._loaded_extensions.append('plotly')


def _activate_plotly_backend(renderer):
    if renderer == "plotly":
        pn.pane.HoloViews._panes["plotly"] = _PlotlyHoloviewsPane

Store._backend_switch_hooks.append(_activate_plotly_backend)
