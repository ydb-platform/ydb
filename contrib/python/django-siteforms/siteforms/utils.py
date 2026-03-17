from contextlib import contextmanager
from typing import Optional, Union

from django.forms import Field

if False:  # pragma: nocover
    from .base import TypeSubform  # noqa


UNSET = set()
"""Value is not set sentinel."""


def merge_dict(src: Optional[dict], dst: Union[dict, str]) -> dict:

    if src is None:
        src = {}

    if isinstance(dst, str):
        # Strings are possible when base layout (e.g. ALL_FIELDS)
        # replaced by user-defined layout.
        return src.copy()

    out = dst.copy()

    for k, v in src.items():
        if isinstance(v, dict):
            v = merge_dict(v, dst.setdefault(k, {}))
        out[k] = v

    return out


def bind_subform(*, subform: 'TypeSubform', field: Field):
    """Initializes field attributes thus linking them to a subform.

    :param subform:
    :param field:

    """
    field.widget.form = subform
    field.form = subform


@contextmanager
def temporary_fields_patch(form):
    """Too bad. Since Django's BoundField uses form base fields attributes,
    (but not its own, e.g. for disabled in .build_widget_attrs())
    we are forced to store and restore previous values not to have side
    effects on form classes reuse.

    .. warning:: Possible race condition. Maybe fix that someday?

    :param form:

    """
    originals = {}

    for name, field in form.base_fields.items():
        originals[name] = (field.widget, field.disabled)

    try:
        yield

    finally:
        for name, field in form.base_fields.items():
            field.widget, field.disabled = originals[name]
