import json

from django.db.models import JSONField
from django.utils.html import format_html

from ..utils import AttributeDict
from .base import library
from .linkcolumn import BaseLinkColumn

try:
    from django.contrib.postgres.fields import HStoreField

    POSTGRES_AVAILABLE = True
except ImportError:
    # psycopg2 is not available, cannot import from django.contrib.postgres.
    # JSONColumn might still be useful to add manually.
    POSTGRES_AVAILABLE = False


@library.register
class JSONColumn(BaseLinkColumn):
    """
    Render the contents of `~django.contrib.postgres.fields.JSONField` or
    `~django.contrib.postgres.fields.HStoreField` as an indented string.

    .. versionadded :: 1.5.0

    .. note::

        Automatic rendering of data to this column requires PostgreSQL support
        (psycopg2 installed) to import the fields, but this column can also be
        used manually without it.

    Arguments:
        json_dumps_kwargs: kwargs passed to `json.dumps`, defaults to `{'indent': 2}`
        attrs (dict): In addition to *attrs* keys supported by `~.Column`, the
            following are available:

             - ``pre`` -- ``<pre>`` around the rendered JSON string in ``<td>`` elements.

    """

    def __init__(self, json_dumps_kwargs=None, **kwargs):
        self.json_dumps_kwargs = (
            json_dumps_kwargs if json_dumps_kwargs is not None else {"indent": 2}
        )

        super().__init__(**kwargs)

    def render(self, record, value):
        return format_html(
            "<pre {}>{}</pre>",
            AttributeDict(self.attrs.get("pre", {})).as_html(),
            json.dumps(value, **self.json_dumps_kwargs),
        )

    @classmethod
    def from_field(cls, field, **kwargs):
        if POSTGRES_AVAILABLE:
            if isinstance(field, (JSONField, HStoreField)):
                return cls(**kwargs)
