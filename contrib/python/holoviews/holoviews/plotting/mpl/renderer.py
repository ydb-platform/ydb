import base64
import os
from contextlib import contextmanager, suppress
from io import BytesIO
from itertools import chain
from tempfile import NamedTemporaryFile

import matplotlib as mpl
import numpy as np
import param
from matplotlib import pyplot as plt
from param.parameterized import bothmethod

from ...core import HoloMap
from ...core.options import Store
from ..renderer import HTML_TAGS, MIME_TYPES, Renderer
from .util import get_old_rcparams, get_tight_bbox


class OutputWarning(param.Parameterized):pass
outputwarning = OutputWarning(name='Warning')

# <format name> : (animation writer, format,  anim_kwargs, extra_args)
ANIMATION_OPTS = {
    'webm': ('ffmpeg', 'webm', {},
             ['-vcodec', 'libvpx-vp9', '-b', '1000k']),
    'mp4': ('ffmpeg', 'mp4', {'codec': 'libx264'},
            ['-pix_fmt', 'yuv420p']),
    'gif': ('pillow', 'gif', {'fps': 10}, []),
    'scrubber': ('html', None, {'fps': 5}, None)
}


class MPLRenderer(Renderer):
    """Exporter used to render data from matplotlib, either to a stream
    or directly to file.

    The __call__ method renders an HoloViews component to raw data of
    a specified matplotlib format.  The save method is the
    corresponding method for saving a HoloViews objects to disk.

    The save_fig and save_anim methods are used to save matplotlib
    figure and animation objects. These match the two primary return
    types of plotting class implemented with matplotlib.

    """

    drawn = {}

    backend = param.String('matplotlib', doc="The backend name.")

    dpi=param.Integer(default=72, doc="""
        The render resolution in dpi (dots per inch)""")

    fig = param.Selector(default='auto',
                               objects=['png', 'svg', 'pdf', 'pgf',
                                        'html', None, 'auto'], doc="""
        Output render format for static figures. If None, no figure
        rendering will occur. """)

    holomap = param.Selector(default='auto',
                                   objects=['widgets', 'scrubber', 'webm','mp4', 'gif', None, 'auto'], doc="""
        Output render multi-frame (typically animated) format. If
        None, no multi-frame rendering will occur.""")

    interactive = param.Boolean(default=False, doc="""
        Whether to enable interactive plotting allowing interactive
        plotting with explicitly calling show.""")

    mode = param.Selector(default='default', objects=['default'])


    mode_formats = {'fig':     ['png', 'svg', 'pdf', 'pgf', 'html', None, 'auto'],
                    'holomap': ['widgets', 'scrubber', 'webm','mp4', 'gif',
                                'html', None, 'auto']}

    counter = 0

    def show(self, obj):
        """Renders the supplied object and displays it using the active
        GUI backend.

        """
        if self.interactive:
            if isinstance(obj, list):
                return [self.get_plot(o) for o in obj]
            return self.get_plot(obj)

        from .plot import MPLPlot
        MPLPlot._close_figures = False
        try:
            plots = []
            objects = obj if isinstance(obj, list) else [obj]
            for o in objects:
                plots.append(self.get_plot(o))
            plt.show()
        except Exception:
            raise
        finally:
            MPLPlot._close_figures = True
        return plots[0] if len(plots) == 1 else plots


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
        from .plot import MPLPlot
        factor = percent_size / 100.0
        obj = obj.last if isinstance(obj, HoloMap) else obj
        options = Store.lookup_options(cls.backend, obj, 'plot').options
        fig_size = options.get('fig_size', MPLPlot.fig_size)*factor

        return dict({'fig_size':fig_size},
                    **MPLPlot.lookup_options(obj, 'plot').options)


    @bothmethod
    def get_size(self_or_cls, plot):
        w, h = plot.state.get_size_inches()
        dpi = self_or_cls.dpi if self_or_cls.dpi else plot.state.dpi
        return (int(w*dpi), int(h*dpi))


    def _figure_data(self, plot, fmt, bbox_inches='tight', as_script=False, **kwargs):
        """Render matplotlib figure object and return the corresponding
        data.  If as_script is True, the content will be split in an
        HTML and a JS component.

        Similar to IPython.core.pylabtools.print_figure but without
        any IPython dependency.

        """
        if fmt in ['gif', 'mp4', 'webm']:
            with mpl.rc_context(rc=plot.fig_rcparams):
                anim = plot.anim(fps=self.fps)
            data = self._anim_data(anim, fmt)
        else:
            fig = plot.state

            traverse_fn = lambda x: x.handles.get('bbox_extra_artists', None)
            extra_artists = list(
                chain.from_iterable(artists for artists in plot.traverse(traverse_fn)
                                    if artists is not None)
            )

            kw = dict(
                format=fmt,
                facecolor=fig.get_facecolor(),
                edgecolor=fig.get_edgecolor(),
                dpi=self.dpi,
                bbox_inches=bbox_inches,
                bbox_extra_artists=extra_artists
            )
            kw.update(kwargs)

            with np.errstate(invalid="ignore"):
                with suppress(Exception):
                    # Attempts to precompute the tight bounding box
                    kw = self._compute_bbox(fig, kw)
                bytes_io = BytesIO()
                fig.canvas.print_figure(bytes_io, **kw)
            data = bytes_io.getvalue()

        if as_script:
            b64 = base64.b64encode(data).decode("utf-8")
            (mime_type, tag) = MIME_TYPES[fmt], HTML_TAGS[fmt]
            src = HTML_TAGS['base64'].format(mime_type=mime_type, b64=b64)
            html = tag.format(src=src, mime_type=mime_type, css='')
            return html
        if fmt == 'svg':
            data = data.decode('utf-8')
        return data


    def _anim_data(self, anim, fmt):
        """Render a matplotlib animation object and return the corresponding data.

        """
        (writer, _, anim_kwargs, extra_args) = ANIMATION_OPTS[fmt]
        if extra_args != []:
            anim_kwargs = dict(anim_kwargs, extra_args=extra_args)

        if self.fps is not None: anim_kwargs['fps'] = max([int(self.fps), 1])
        if self.dpi is not None: anim_kwargs['dpi'] = self.dpi
        if not hasattr(anim, '_encoded_video'):
            # Windows will throw PermissionError with auto-delete
            with NamedTemporaryFile(suffix=f'.{fmt}', delete=False) as f:
                anim.save(f.name, writer=writer, **anim_kwargs)
                video = f.read()
            f.close()
            os.remove(f.name)
        return video


    def _compute_bbox(self, fig, kw):
        """Compute the tight bounding box for each figure once, reducing
        number of required canvas draw calls from N*2 to N+1 as a
        function of the number of frames.

        Tight bounding box computing code here mirrors:
        matplotlib.backend_bases.FigureCanvasBase.print_figure
        as it hasn't been factored out as a function.

        """
        fig_id = id(fig)
        if kw['bbox_inches'] == 'tight':
            if fig_id not in MPLRenderer.drawn:
                fig.set_dpi(self.dpi)
                fig.canvas.draw()
                extra_artists = kw.pop("bbox_extra_artists", [])
                pad = mpl.rcParams['savefig.pad_inches']
                bbox_inches = get_tight_bbox(fig, extra_artists, pad=pad)
                MPLRenderer.drawn[fig_id] = bbox_inches
                kw['bbox_inches'] = bbox_inches
            else:
                kw['bbox_inches'] = MPLRenderer.drawn[fig_id]
        return kw

    @classmethod
    @contextmanager
    def state(cls):
        old_rcparams = get_old_rcparams()
        try:
            cls._rcParams = old_rcparams
            yield
        finally:
            mpl.rcParams.clear()
            mpl.rcParams.update(cls._rcParams)


    @classmethod
    def load_nb(cls, inline=True):
        """Initialize matplotlib backend

        """
        import matplotlib.pyplot as plt
        backend = plt.get_backend()
        if backend not in ['agg', 'module://ipykernel.pylab.backend_inline']:
            plt.switch_backend('agg')
