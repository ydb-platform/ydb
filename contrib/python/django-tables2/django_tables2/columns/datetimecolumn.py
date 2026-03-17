from django.db import models

from .base import library
from .templatecolumn import TemplateColumn


@library.register
class DateTimeColumn(TemplateColumn):
    """
    A column that renders `datetime` instances in the local timezone.

    Arguments:
        format (str): format string for datetime (optional).
                      Note that *format* uses Django's `date` template tag syntax.
        short (bool): if `format` is not specified, use Django's
                      ``SHORT_DATETIME_FORMAT``, else ``DATETIME_FORMAT``
    """

    def __init__(self, format=None, short=True, *args, **kwargs):
        if format is None:
            format = "SHORT_DATETIME_FORMAT" if short else "DATETIME_FORMAT"
        template = '{{ value|date:"%s"|default:default }}' % format  # noqa: UP031
        super().__init__(template_code=template, *args, **kwargs)

    @classmethod
    def from_field(cls, field, **kwargs):
        if isinstance(field, models.DateTimeField):
            return cls(**kwargs)
