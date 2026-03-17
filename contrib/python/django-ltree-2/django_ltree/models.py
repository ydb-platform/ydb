from django.db import models
from typing import Any

from .fields import PathField, PathValue
from .managers import TreeManager


class TreeModel(models.Model):
    path = PathField(unique=True)
    objects = TreeManager()

    class Meta:
        abstract = True
        ordering = ("path",)

    def label(self):
        return self.path[-1]

    def get_ancestors_paths(self) -> list[list[str]]:
        return [PathValue(self.path[:n]) for n, p in enumerate(self.path) if n > 0]

    def ancestors(self):
        return type(self)._default_manager.filter(path__ancestors=self.path)

    def descendants(self):
        return type(self)._default_manager.filter(path__descendants=self.path)

    def parent(self):
        if len(self.path) > 1:
            return self.ancestors().exclude(id=self.id).last()

    def children(self):
        return self.descendants().filter(path__depth=len(self.path) + 1)

    def siblings(self):
        parent = self.path[:-1]
        return (
            type(self)
            ._default_manager.filter(path__descendants=".".join(parent))
            .filter(path__depth=len(self.path))
            .exclude(path=self.path)
        )

    def add_child(self, slug: str, **kwargs) -> Any:
        assert "path" not in kwargs
        kwargs["path"] = self.path[:]
        kwargs["path"].append(slug)
        kwargs["slug"] = slug
        return type(self)._default_manager.create(**kwargs)
