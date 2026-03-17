from dataclasses import dataclass, field
from typing import Any

from django.forms.utils import flatatt
from django.templatetags.static import static
from django.utils.functional import lazy
from django.utils.html import format_html, html_safe, json_script, mark_safe


__all__ = ["CSS", "ImportMap", "JS", "JSON", "importmap", "static", "static_lazy"]


def static_if_relative(path):
    return path if path.startswith(("http://", "https://", "/")) else static(path)


static_lazy = lazy(static, str)


@html_safe
@dataclass(eq=True)
class CSS:
    src: str
    inline: bool = field(default=False, kw_only=True)
    media: str = "all"

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        if self.inline:
            return format_html('<style media="{}">{}</style>', self.media, self.src)
        return format_html(
            '<link href="{}" media="{}" rel="stylesheet">',
            static_if_relative(self.src),
            self.media,
        )


@html_safe
@dataclass(eq=True)
class JS:
    src: str
    attrs: dict[str, Any] = field(default_factory=dict)

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return format_html(
            '<script src="{}"{}></script>',
            static_if_relative(self.src),
            mark_safe(flatatt(self.attrs)),
        )


@html_safe
@dataclass(eq=True)
class JSON:
    data: dict[str, Any]
    id: str | None = field(default="", kw_only=True)

    def __hash__(self):
        return hash(self.__str__())

    def __str__(self):
        return json_script(self.data, self.id)


@html_safe
class ImportMap:
    def __init__(self, importmap):
        self._importmap = importmap

    def __str__(self):
        if self._importmap:
            html = json_script(self._importmap).removeprefix(
                '<script type="application/json">'
            )
            return mark_safe(f'<script type="importmap">{html}')
        return ""

    def update(self, other):
        if isinstance(other, ImportMap):
            other = other._importmap

        if imports := other.get("imports"):
            self._importmap.setdefault("imports", {}).update(imports)
        if integrity := other.get("integrity"):
            self._importmap.setdefault("integrity", {}).update(integrity)
        if scopes := other.get("scopes"):
            for scope, imports in scopes.items():
                self._importmap.setdefault("scopes", {}).setdefault(scope, {}).update(
                    imports
                )

    def __or__(self, other):
        if isinstance(other, ImportMap):
            combined = self.__class__({})
            combined.update(self)
            combined.update(other)
            return combined
        return NotImplemented


importmap = ImportMap({})
