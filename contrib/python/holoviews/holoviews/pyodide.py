import asyncio
import sys

from bokeh.document import Document
from bokeh.embed.elements import script_for_render_items
from bokeh.embed.util import standalone_docs_json_and_render_items
from bokeh.embed.wrappers import wrap_in_script_tag
from panel.io.pyodide import _link_docs
from panel.pane import panel as as_panel

from .core.dimension import LabelledData
from .core.options import Store
from .util import extension as _extension

#-----------------------------------------------------------------------------
# Private API
#-----------------------------------------------------------------------------
_background_task = set()

async def _link(ref, doc):
    from js import Bokeh
    rendered = Bokeh.index.object_keys()
    if ref not in rendered:
        await asyncio.sleep(0.1)
        await _link(ref, doc)
        return
    views = Bokeh.index.object_values()
    view = views[rendered.indexOf(ref)]
    _link_docs(doc, view.model.document)

def render_html(obj):
    from js import document
    if hasattr(sys.stdout, '_out'):
        target = sys.stdout._out # type: ignore
    else:
        raise ValueError("Could not determine target node to write to.")
    doc = Document()
    as_panel(obj).server_doc(doc, location=False)
    docs_json, [render_item,] = standalone_docs_json_and_render_items(
        doc.roots, suppress_callback_warning=True
    )
    for root in doc.roots:
        render_item.roots._roots[root] = target
    document.getElementById(target).classList.add('bk-root')
    script = script_for_render_items(docs_json, [render_item])
    task = asyncio.create_task(_link(doc.roots[0].ref['id'], doc))
    _background_task.add(task)
    task.add_done_callback(_background_task.discard)
    return {'text/html': wrap_in_script_tag(script)}, {}

def render_image(element, fmt):
    """Used to render elements to an image format (svg or png) if requested
    in the display formats.

    """
    if fmt not in Store.display_formats:
        return None

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

def render_png(element):
    return render_image(element, 'png')

def render_svg(element):
    return render_image(element, 'svg')

def in_jupyterlite():
    import js
    return hasattr(js, "_JUPYTERLAB") or hasattr(js, "webpackChunk_jupyterlite_pyodide_kernel_extension") or not hasattr(js, "document")

#-----------------------------------------------------------------------------
# Public API
#-----------------------------------------------------------------------------

class pyodide_extension(_extension):

    _loaded = False

    def __call__(self, *args, **params):
        super().__call__(*args, **params)
        if not self._loaded:
            Store.output_settings.initialize(list(Store.renderers.keys()))
            Store.set_display_hook('html+js', LabelledData, render_html)
            Store.set_display_hook('png', LabelledData, render_png)
            Store.set_display_hook('svg', LabelledData, render_svg)
            pyodide_extension._loaded = True
