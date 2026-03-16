"""
Github flavored markdown
~~~~~~~~~~~~~~~~~~~~~~~~

https://github.github.com/gfm

Unlike other extensions, GFM provides a self-contained subclass of ``Markdown``
with parser and renderer already set.
User may also use the parser and renderer as bases for further extension.

Example usage::

    from marko.ext.gfm import gfm
    print(gfm(text))

"""

from marko import Markdown
from marko.helpers import MarkoExtension

from . import elements, renderer

GFM = MarkoExtension(
    elements=[
        elements.Paragraph,
        elements.Strikethrough,
        elements.Url,
        elements.Table,
        elements.TableRow,
        elements.TableCell,
        elements.Alert,
    ],
    renderer_mixins=[renderer.GFMRendererMixin],
)


gfm = Markdown(extensions=[GFM])


def make_extension():
    return GFM
