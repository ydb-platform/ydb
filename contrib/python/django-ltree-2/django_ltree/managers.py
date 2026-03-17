from django.db import models
from typing import TYPE_CHECKING
from django_ltree.paths import PathGenerator

from .querysets import TreeQuerySet

if TYPE_CHECKING:
    from django_ltree.models import TreeModel


class TreeManager(models.Manager):
    def get_queryset(self) -> TreeQuerySet["TreeModel"]:
        """Returns a queryset with the models ordered by `path`"""
        return TreeQuerySet(model=self.model, using=self._db).order_by("path")

    def roots(self) -> TreeQuerySet["TreeModel"]:
        """Returns the roots of a given model"""
        return self.filter().roots()

    def children(self, path: str) -> TreeQuerySet["TreeModel"]:
        """Returns the childrens of a given object"""
        return self.filter().children(path)

    def create_child(
        self, parent: "TreeModel" = None, label: str = None, **kwargs
    ) -> TreeQuerySet["TreeModel"]:
        """Creates a tree child with or without parent"""
        prefix = parent.path if parent else None

        """If a label is not provided, we generate a new one, else we use it as suffix"""
        if label is None:
            paths_in_use = parent.children() if parent else self.roots()
            path_generator = PathGenerator(
                prefix,
                skip=paths_in_use.values_list("path", flat=True),
            )
            path = next(path_generator)
        else:
            if prefix is None:
                path = label
            else:
                path = str(prefix) + "." + label

        kwargs["path"] = path
        return self.create(**kwargs)
