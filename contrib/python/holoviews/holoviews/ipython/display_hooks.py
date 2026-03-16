"""Definition and registration of display hooks for the IPython Notebook.

"""
import sys
import traceback
from contextlib import contextmanager
from functools import wraps

import IPython
from IPython import get_ipython
from IPython.display import HTML

import holoviews as hv

from ..core import (
    AdjointLayout,
    CompositeOverlay,
    Dimensioned,
    DynamicMap,
    Empty,
    GridSpace,
    HoloMap,
    Layout,
    NdLayout,
    ViewableElement,
)
from ..core.io import FileArchive
from ..core.options import AbbreviatedException, SkipRendering, Store, StoreOptions
from ..core.traversal import unique_dimkeys
from ..core.util import mimebundle_to_html
from ..util.settings import OutputSettings
from .magics import OptsMagic, OutputMagic

# To assist with debugging of display hooks
FULL_TRACEBACK = None
ABBREVIATE_TRACEBACKS = True

#==================#
# Helper functions #
#==================#


def max_frame_warning(max_frames):
    sys.stderr.write(
        f"Animation longer than the max_frames limit {max_frames};\n"
        "skipping rendering to avoid unexpected lengthy computations.\n"
        "If desired, the limit can be increased using:\n"
        "hv.output(max_frames=<insert number>)"
    )

def process_object(obj):
    """Hook to process the object currently being displayed.

    """
    invalid_options = OptsMagic.process_element(obj)
    if invalid_options: return invalid_options
    OutputMagic.info(obj)


def render(obj, **kwargs):
    info = process_object(obj)
    if info:
        display(HTML(info))
        return

    if render_anim is not None:
        return render_anim(obj)

    backend = Store.current_backend
    renderer = Store.renderers[backend]

    # Drop back to png if pdf selected, notebook PDF rendering is buggy
    if renderer.fig == 'pdf':
        renderer = renderer.instance(fig='png')

    return renderer.components(obj, **kwargs)


def single_frame_plot(obj):
    """Returns plot, renderer and format for single frame export.

    """
    obj = Layout(obj) if isinstance(obj, AdjointLayout) else obj

    backend = Store.current_backend
    renderer = Store.renderers[backend]

    plot_cls = renderer.plotting_class(obj)
    plot = plot_cls(obj, **renderer.plot_options(obj, renderer.size))
    fmt = (renderer.param.objects('existing')['fig'].objects[0]
           if renderer.fig == 'auto' else renderer.fig)
    return plot, renderer, fmt


def first_frame(obj):
    """Only display the first frame of an animated plot

    """
    plot, renderer, fmt = single_frame_plot(obj)
    plot.update(0)
    return {'text/html': renderer.html(plot, fmt)}

def middle_frame(obj):
    """Only display the (approximately) middle frame of an animated plot

    """
    plot, renderer, fmt = single_frame_plot(obj)
    middle_frame = int(len(plot) / 2)
    plot.update(middle_frame)
    return {'text/html': renderer.html(plot, fmt)}

def last_frame(obj):
    """Only display the last frame of an animated plot

    """
    plot, renderer, fmt = single_frame_plot(obj)
    plot.update(len(plot))
    return {'text/html': renderer.html(plot, fmt)}

#===============#
# Display hooks #
#===============#

def dynamic_optstate(element, state=None):
    # Temporary fix to avoid issues with DynamicMap traversal
    DynamicMap._deep_indexable = False
    optstate = StoreOptions.state(element,state=state)
    DynamicMap._deep_indexable = True
    return optstate

@contextmanager
def option_state(element):
    optstate = dynamic_optstate(element)
    try:
        yield
    except Exception:
        dynamic_optstate(element, state=optstate)
        raise

def _render_jupyter_exception(e):
    import html

    raw_tb = "\n".join(traceback.format_exception(e.etype, e.value, e.traceback)).strip()

    escaped_tb = html.escape(raw_tb)
    exc_name = e.etype.__name__
    exc_msg = str(e.value).split("\n")[0]
    if len(exc_msg) > 80:
        exc_msg = exc_msg[:80].strip() + " ..."
    exc_msg = html.escape(exc_msg)

    output_html = f"""
    <style>
      .hv-jupyter-exc {{
        font-family: monospace;
        border-radius: 3px;
        padding: 0.5em;
        margin: 0.5em 0;
      }}
      .hv-jupyter-exc summary {{
        cursor: pointer;
        user-select: none;
        list-style: none;
        position: relative;
        padding-left: 1.2em;
      }}
      .hv-jupyter-exc summary::after {{
        content: "▶";
        position: absolute;
        left: 0;
        top: 0;
        transition: transform 0.2s ease;
      }}
      .hv-jupyter-exc[open] summary::after {{
        transform: rotate(90deg);
      }}
      .hv-jupyter-full {{
        padding: 0.5em 0 0 1em;
        white-space: pre-wrap;
        background-color: #fdd;
        color: #000;
      }}
    </style>
    <details class="hv-jupyter-exc">
      <summary><b>{exc_name}:</b> {exc_msg}</summary>
      <div class="hv-jupyter-full">{escaped_tb}</pre>
    </details>"""
    return {"text/html": output_html}


def display_hook(fn):
    """A decorator to wrap display hooks that return a MIME bundle or None.
    Additionally it handles adding output to the notebook archive, saves
    files specified with the output magic and handles tracebacks.

    """
    @wraps(fn)
    def wrapped(element):
        global FULL_TRACEBACK  # noqa: PLW0603
        if Store.current_backend is None:
            return {}, {}

        try:
            max_frames = OutputSettings.options['max_frames']
            mimebundle = fn(element, max_frames=max_frames)
            if mimebundle is None:
                return {}, {}
            mime_data, mime_metadata = mimebundle
            from ..plotting.renderer import MIME_TYPES
            if MIME_TYPES['js'] in mime_data:
                mime_data['text/html'] = mimebundle_to_html(mime_data)
                del mime_data[MIME_TYPES['js']]

            # Only want to add to the archive for one display hook...
            disabled_suffixes = ['png_display', 'svg_display']
            if not any(fn.__name__.endswith(suffix) for suffix in disabled_suffixes):
                if type(hv.archive) is not FileArchive and 'text/html' in mime_data:
                    hv.archive.add(element, html=mime_data['text/html'])
            filename = OutputSettings.options['filename']
            if filename:
                Store.renderers[Store.current_backend].save(element, filename)
            return mime_data, mime_metadata
        except SkipRendering as e:
            if e.warn:
                sys.stderr.write(str(e))
            return {}, {}
        except AbbreviatedException as e:
            FULL_TRACEBACK = '\n'.join(traceback.format_exception(e.etype, e.value, e.traceback))
            return _render_jupyter_exception(e), {}
        except Exception:
            raise
    return wrapped


@display_hook
def element_display(element, max_frames):
    info = process_object(element)
    if info:
        display(HTML(info))
        return None

    backend = Store.current_backend
    if type(element) not in Store.registry[backend]:
        return None

    return render(element)


@display_hook
def map_display(vmap, max_frames):
    if not isinstance(vmap, (HoloMap, DynamicMap)): return None

    if len(vmap) == 0 and not isinstance(vmap, DynamicMap):
        return None

    elif len(vmap) > max_frames:
        max_frame_warning(max_frames)
        return None

    return render(vmap)


@display_hook
def layout_display(layout, max_frames):
    if isinstance(layout, AdjointLayout):
        layout = Layout(layout).opts(layout.opts.get('plot'))
    if not isinstance(layout, (Layout, NdLayout)): return None

    nframes = len(unique_dimkeys(layout)[1])
    if nframes > max_frames:
        max_frame_warning(max_frames)
        return None

    return render(layout)


@display_hook
def grid_display(grid, max_frames):
    if not isinstance(grid, GridSpace): return None

    nframes = len(unique_dimkeys(grid)[1])
    if nframes > max_frames:
        max_frame_warning(max_frames)
        return None

    return render(grid)


def display(obj, raw_output=False, **kwargs):
    """Renders any HoloViews object to HTML and displays it
    using the IPython display function. If raw is enabled
    the raw HTML is returned instead of displaying it directly.

    """
    if not Store.loaded_backends() and isinstance(obj, Dimensioned):
        raise RuntimeError('To use display on a HoloViews object ensure '
                           'a backend is loaded using the holoviews '
                           'extension.')

    from ..plotting import Plot

    raw = True
    if isinstance(obj, GridSpace):
        with option_state(obj):
            output = grid_display(obj)
    elif isinstance(obj, (CompositeOverlay, ViewableElement)):
        with option_state(obj):
            output = element_display(obj)
    elif isinstance(obj, (Layout, NdLayout, AdjointLayout)):
        with option_state(obj):
            output = layout_display(obj)
    elif isinstance(obj, (HoloMap, DynamicMap)):
        with option_state(obj):
            output = map_display(obj)
    elif isinstance(obj, Plot):
        output = render(obj)
    elif isinstance(obj, Empty):
        output = ({}, {})
    else:
        output = obj
        raw = kwargs.pop('raw', False)

    if raw_output:
        return output
    elif isinstance(output, tuple):
        data, metadata = output
    else:
        data, metadata = output, {}
    return IPython.display.display(data, raw=raw, metadata=metadata, **kwargs)


def pprint_display(obj):
    if 'html' not in Store.display_formats:
        return None

    # If pretty printing is off, return None (fallback to next display format)
    ip = get_ipython()
    if not ip.display_formatter.formatters['text/plain'].pprint:
        return None
    return display(obj, raw_output=True)


def image_display(element, max_frames, fmt):
    """Used to render elements to an image format (svg or png) if requested
    in the display formats.

    """
    if fmt not in Store.display_formats:
        return None
    info = process_object(element)
    if info:
        display(HTML(info))
        return

    backend = Store.current_backend
    if type(element) not in Store.registry[backend]:
        return None
    renderer = Store.renderers[backend]
    plot = renderer.get_plot(element)

    # Current renderer does not support the image format
    if fmt not in renderer.param.objects('existing')['fig'].objects:
        return None

    data, info = renderer(plot, fmt=fmt)
    return {info['mime_type']: data}, {}


@display_hook
def png_display(element, max_frames):
    """Used to render elements to PNG if requested in the display formats.

    """
    return image_display(element, max_frames, fmt='png')


@display_hook
def svg_display(element, max_frames):
    """Used to render elements to SVG if requested in the display formats.

    """
    return image_display(element, max_frames, fmt='svg')


# display_video output by default, but may be set to first_frame,
# middle_frame or last_frame (e.g. for testing purposes)
render_anim = None

def plot_display(plot):
    return plot.renderer.components(plot)
