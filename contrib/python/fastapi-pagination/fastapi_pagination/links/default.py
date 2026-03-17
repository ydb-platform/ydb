from __future__ import annotations

__all__ = [
    "Page",
    "UseHeaderLinks",
    "UseLinks",
]

from abc import ABC
from math import ceil
from typing import Any, TypeAlias

from typing_extensions import TypeVar

from fastapi_pagination.customization import CustomizedPage
from fastapi_pagination.default import Page as BasePage

from .bases import BaseLinksCustomizer, BaseUseHeaderLinks, BaseUseLinks, Links, create_links

TAny = TypeVar("TAny", default=Any)


def resolve_default_links(_page: BasePage, /, only_path: bool | None = None) -> Links:
    page, size, total = _page.page, _page.size, _page.total

    return create_links(
        first={"page": 1},
        last={"page": ceil(total / size) if total and size else 1},
        next={"page": page + 1}
        if page is not None and size is not None and total is not None and page * size < total
        else None,
        prev={"page": page - 1} if page is not None and page - 1 >= 1 else None,
        only_path=only_path,
    )


class DefaultLinksCustomizer(BaseLinksCustomizer, ABC):
    def resolve_links(self, _page: BasePage, /) -> Links:
        return resolve_default_links(_page, only_path=self.only_path)


class UseLinks(DefaultLinksCustomizer, BaseUseLinks):
    pass


class UseHeaderLinks(DefaultLinksCustomizer, BaseUseHeaderLinks):
    pass


Page: TypeAlias = CustomizedPage[
    BasePage[TAny],
    UseLinks(),
]
