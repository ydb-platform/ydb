from django.db import models
from django.db.migrations import state
from django.db.models import options

from .query import QuerySet
from .sql import Query
from clickhouse_backend import compat

options.DEFAULT_NAMES = (
    *options.DEFAULT_NAMES,
    # Clickhouse features
    "engine",
    "cluster",
)
# Also monkey patch state.DEFAULT_NAMES, this makes new option names contained in migrations.
state.DEFAULT_NAMES = options.DEFAULT_NAMES


class ClickhouseManager(models.Manager):
    _queryset_class = QuerySet

    def get_queryset(self):
        """
        User defined Query and QuerySet class that support clickhouse particular query.
        """
        return self._queryset_class(
            model=self.model, query=Query(self.model), using=self._db, hints=self._hints
        )

    def settings(self, **kwargs):
        return self.get_queryset().settings(**kwargs)

    def prewhere(self, *args, **kwargs):
        return self.get_queryset().prewhere(*args, **kwargs)

    def datetimes(self, *args, **kwargs):
        return self.get_queryset().datetimes(*args, **kwargs)


class ClickhouseModel(models.Model):
    objects = ClickhouseManager()
    _overwrite_base_manager = ClickhouseManager()

    class Meta:
        abstract = True
        base_manager_name = "_overwrite_base_manager"

    if compat.dj_ge6:

        def _do_update(
            self,
            base_qs,
            using,
            pk_val,
            values,
            update_fields,
            forced_update,
            returning_fields,
        ):
            """
            Try to update the model. Return True if the model was updated (if an
            update query was done and a matching row was found in the DB).
            """
            filtered = base_qs.filter(pk=pk_val)
            if not values:
                if update_fields is not None or filtered.exists():
                    return [()]
                return []
            return filtered._update(values, returning_fields)

    else:

        def _do_update(
            self, base_qs, using, pk_val, values, update_fields, forced_update
        ):
            """
            Try to update the model. Return True if the model was updated (if an
            update query was done and a matching row was found in the DB).
            """
            filtered = base_qs.filter(pk=pk_val)
            if not values:
                # We can end up here when saving a model in inheritance chain where
                # update_fields doesn't target any field in current model. In that
                # case we just say the update succeeded. Another case ending up here
                # is a model with just PK - in that case check that the PK still
                # exists.
                return update_fields is not None or filtered.exists()
            return filtered._update(values) > 0
