from django.core import checks
from django.core.exceptions import ImproperlyConfigured


class FieldMixin:
    """All clickhouse field should inherit this mixin.

    1. Remove unsupported arguments: unique, db_index, unique_for_date,
       unique_for_month, unique_for_year, db_tablespace, db_collation.
    2. Return shortened name in deconstruct method.

    low_cardinality argument is added separately in every specific field that support LowCardinality.
    If added in this mixin, then PyCharm will not supply argument hints.
    """

    nullable_allowed = True

    def check(self, **kwargs):
        return [
            *super().check(**kwargs),
            *self._check_nullable(),
        ]

    def _check_nullable(self):
        if self.null and not self.nullable_allowed:
            return [
                checks.Error(
                    "Nullable is not supported by %s." % self.__class__.__name__,
                    obj=self,
                )
            ]
        return []

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        for key in [
            "unique",
            "db_index",
            "unique_for_date",
            "unique_for_month",
            "unique_for_year",
            "db_tablespace",
            "db_collation",
        ]:
            try:
                del kwargs[key]
            except KeyError:
                pass

        if getattr(self, "low_cardinality", False):
            kwargs["low_cardinality"] = self.low_cardinality
        if path.startswith("clickhouse_backend.models.fields"):
            path = path.replace(
                "clickhouse_backend.models.fields", "clickhouse_backend.models"
            )
        return name, path, args, kwargs

    def _nested_type(self, value):
        if value is not None:
            # LowCardinality and Nullable sequence matters, reversal will cause DB::Exception:
            # Nested type LowCardinality(Int8) cannot be inside Nullable type. (ILLEGAL_TYPE_OF_ARGUMENT)
            if self.null:
                value = "Nullable(%s)" % value
            if getattr(self, "low_cardinality", False):
                value = "LowCardinality(%s)" % value
        return value

    def _check_backend(self, connection):
        if connection.vendor != "clickhouse":
            raise ImproperlyConfigured(
                "%s must only be used with django clickhouse backend."
                % self.__class__.__name__
            )

    def db_type(self, connection):
        self._check_backend(connection)
        return self._nested_type(super().db_type(connection))
