from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from starlette.datastructures import URL


@dataclass
class PageControl:
    number: int
    url: str


@dataclass
class Pagination:
    rows: list[Any]
    page: int
    page_size: int
    count: int
    page_controls: list[PageControl] = field(default_factory=list)
    max_page_controls: int = 7

    @property
    def has_previous(self) -> bool:
        return self.page > 1

    @property
    def has_next(self) -> bool:
        next_page = (self.page + 1) * self.page_size
        return next_page <= self.count or next_page - self.count < self.page_size

    @property
    def previous_page(self) -> PageControl:
        for page_control in self.page_controls:
            if page_control.number == self.page - 1:
                return page_control

        raise RuntimeError("Previous page not found.")

    @property
    def next_page(self) -> PageControl:
        for page_control in self.page_controls:
            if page_control.number == self.page + 1:
                return page_control

        raise RuntimeError("Next page not found.")

    def __post_init__(self) -> None:
        # Clamp page
        self.page = min(self.page, max(1, self.count // self.page_size + 1))

    def resize(self, page_size: int) -> Pagination:
        self.page = (self.page - 1) * self.page_size // page_size + 1
        self.page_size = page_size
        return self

    def add_pagination_urls(self, base_url: URL) -> None:
        # Previous pages
        for p in range(self.page - min(self.max_page_controls, 3), self.page):
            if p > 0:
                self._add_page_control(base_url, p)

        # Current page
        self._add_page_control(base_url, self.page)

        # Next pages
        for p in range(self.page + 1, self.page + self.max_page_controls + 1):
            current = p * self.page_size
            if current <= self.count or current - self.count < self.page_size:
                self._add_page_control(base_url, p)

        # Rebalance previous pages if next pages less than 3
        for p in range(self.page - self.max_page_controls - 3, self.page - 3):
            if p > 0:
                self._add_page_control(base_url, p)

        self.page_controls.sort(key=lambda p: p.number)

    def _add_page_control(self, base_url: URL, page: int) -> None:
        self.max_page_controls -= 1

        url = str(base_url.include_query_params(page=page))
        page_control = PageControl(number=page, url=url)
        self.page_controls.append(page_control)
