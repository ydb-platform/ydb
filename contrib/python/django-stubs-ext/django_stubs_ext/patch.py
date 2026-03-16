import builtins
from typing import Any, Generic, Iterable, List, Optional, Tuple, Type, TypeVar

from django import VERSION as VERSION
from django.contrib.admin import ModelAdmin
from django.contrib.admin.options import BaseModelAdmin
from django.contrib.sitemaps import Sitemap
from django.contrib.syndication.views import Feed
from django.core.files.utils import FileProxyMixin
from django.core.paginator import Paginator
from django.db.models.fields import Field
from django.db.models.fields.related import ForeignKey
from django.db.models.lookups import Lookup
from django.db.models.manager import BaseManager
from django.db.models.query import QuerySet
from django.forms.formsets import BaseFormSet
from django.forms.models import BaseModelForm, BaseModelFormSet
from django.views.generic.detail import SingleObjectMixin
from django.views.generic.edit import DeletionMixin, FormMixin
from django.views.generic.list import MultipleObjectMixin

__all__ = ["monkeypatch"]

_T = TypeVar("_T")
_VersionSpec = Tuple[int, int]


class MPGeneric(Generic[_T]):
    """Create a data class to hold metadata about the generic classes needing monkeypatching.

    The `version` param is optional, and a value of `None` means that the monkeypatch is
    version-independent.

    This is slightly overkill for our purposes, but useful for future-proofing against any
    possible issues we may run into with this method.
    """

    def __init__(self, cls: Type[_T], version: Optional[_VersionSpec] = None) -> None:
        """Set the data fields, basic constructor."""
        self.version = version
        self.cls = cls

    def __repr__(self) -> str:
        """Better representation in tests and debug."""
        return "<MPGeneric: {}, versions={}>".format(self.cls, self.version or "all")


# certain django classes need to be generic, but lack the __class_getitem__ dunder needed to
# annotate them: https://github.com/typeddjango/django-stubs/issues/507
# this list stores them so `monkeypatch` can fix them when called
_need_generic: List[MPGeneric[Any]] = [
    MPGeneric(ModelAdmin),
    MPGeneric(SingleObjectMixin),
    MPGeneric(FormMixin),
    MPGeneric(DeletionMixin),
    MPGeneric(MultipleObjectMixin),
    MPGeneric(BaseModelAdmin),
    MPGeneric(Field),
    MPGeneric(Paginator),
    MPGeneric(BaseFormSet),
    MPGeneric(BaseModelForm),
    MPGeneric(BaseModelFormSet),
    MPGeneric(Feed),
    MPGeneric(Sitemap),
    MPGeneric(FileProxyMixin),
    MPGeneric(Lookup),
    # These types do have native `__class_getitem__` method since django 3.1:
    MPGeneric(QuerySet, (3, 1)),
    MPGeneric(BaseManager, (3, 1)),
    # These types do have native `__class_getitem__` method since django 4.1:
    MPGeneric(ForeignKey, (4, 1)),
]


def monkeypatch(extra_classes: Optional[Iterable[type]] = None, include_builtins: bool = True) -> None:
    """Monkey patch django as necessary to work properly with mypy."""

    # Add the __class_getitem__ dunder.
    suited_for_this_version = filter(
        lambda spec: spec.version is None or VERSION[:2] <= spec.version,
        _need_generic,
    )
    for el in suited_for_this_version:
        el.cls.__class_getitem__ = classmethod(lambda cls, *args, **kwargs: cls)
    if extra_classes:
        for cls in extra_classes:
            cls.__class_getitem__ = classmethod(lambda cls, *args, **kwargs: cls)  # type: ignore[attr-defined]

    # Add `reveal_type` and `reveal_locals` helpers if needed:
    if include_builtins:
        builtins.reveal_type = lambda _: None
        builtins.reveal_locals = lambda: None
