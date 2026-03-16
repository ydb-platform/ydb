import typing

if typing.TYPE_CHECKING:
    from django.db.models.query import _T, _QuerySet, _Row
    from django.utils.functional import _StrOrPromise as StrOrPromise
    from django.utils.functional import _StrPromise as StrPromise

    ValuesQuerySet = _QuerySet[_T, _Row]
else:
    from django.db.models.query import QuerySet
    from django.utils.functional import Promise as StrPromise

    ValuesQuerySet = QuerySet
    StrOrPromise = typing.Union[str, StrPromise]

__all__ = ["StrOrPromise", "StrPromise", "ValuesQuerySet"]
