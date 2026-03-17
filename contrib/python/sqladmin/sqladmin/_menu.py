from __future__ import annotations

from typing import TYPE_CHECKING

from starlette.datastructures import URL
from starlette.requests import Request

if TYPE_CHECKING:
    from sqladmin.application import BaseView, ModelView


class ItemMenu:
    def __init__(self, name: str, icon: str | None = None) -> None:
        self.name = name
        self.icon = icon
        self.parent: "ItemMenu" | None = None
        self.children: list["ItemMenu"] = []

    def add_child(self, item: "ItemMenu") -> None:
        item.parent = self
        self.children.append(item)

    def is_visible(self, request: Request) -> bool:
        return True

    def is_accessible(self, request: Request) -> bool:
        return True

    def is_active(self, request: Request) -> bool:
        return False

    def url(self, request: Request) -> str | URL:
        return "#"

    @property
    def display_name(self) -> str:
        return self.name

    @property
    def type_(self) -> str:
        return self.__class__.__name__


class CategoryMenu(ItemMenu):
    def is_active(self, request: Request) -> bool:
        return any(
            c.is_active(request) and c.is_accessible(request) for c in self.children
        )

    @property
    def type_(self) -> str:
        return "Category"


class ViewMenu(ItemMenu):
    def __init__(
        self,
        view: "BaseView" | "ModelView",
        name: str,
        icon: str | None = None,
    ) -> None:
        super().__init__(name=name, icon=icon)
        self.view = view

    def is_visible(self, request: Request) -> bool:
        return self.view.is_visible(request)

    def is_accessible(self, request: Request) -> bool:
        return self.view.is_accessible(request)

    def is_active(self, request: Request) -> bool:
        return self.view.identity == request.path_params.get("identity")

    def url(self, request: Request) -> str | URL:
        if self.view.is_model:
            return request.url_for("admin:list", identity=self.view.identity)
        return request.url_for(f"admin:{self.view.identity}")

    @property
    def display_name(self) -> str:
        return getattr(self.view, "name_plural", None) or self.view.name

    @property
    def type_(self) -> str:
        return "View"


class Menu:
    def __init__(self) -> None:
        self.items: list[ItemMenu] = []

    def add(self, item: ItemMenu) -> None:
        # Only works for one-level menu
        for root in self.items:
            if root.name == item.name:
                root.children.extend(item.children)
                return
        self.items.append(item)
