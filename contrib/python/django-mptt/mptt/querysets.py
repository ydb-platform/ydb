from django.db import models

from mptt import utils


class TreeQuerySet(models.query.QuerySet):
    def as_manager(cls):
        # Address the circular dependency between `Queryset` and `Manager`.
        from mptt.managers import TreeManager

        manager = TreeManager.from_queryset(cls)()
        manager._built_with_as_manager = True
        return manager

    as_manager.queryset_only = True
    as_manager = classmethod(as_manager)

    def get_descendants(self, *args, **kwargs):
        """
        Alias to `mptt.managers.TreeManager.get_queryset_descendants`.
        """
        return self.model._tree_manager.get_queryset_descendants(self, *args, **kwargs)

    get_descendants.queryset_only = True

    def get_ancestors(self, *args, **kwargs):
        """
        Alias to `mptt.managers.TreeManager.get_queryset_ancestors`.
        """
        return self.model._tree_manager.get_queryset_ancestors(self, *args, **kwargs)

    get_ancestors.queryset_only = True

    def get_cached_trees(self):
        """
        Alias to `mptt.utils.get_cached_trees`.
        """
        return utils.get_cached_trees(self)
