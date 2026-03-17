from django.db import models

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .models import TreeModel


class TreeQuerySet(models.QuerySet):
    def roots(self) -> models.QuerySet["TreeModel"]:
        return self.filter(path__depth=1)

    def children(self, path: str) -> models.QuerySet["TreeModel"]:
        return self.filter(path__descendants=path, path__depth=len(path) + 1)
