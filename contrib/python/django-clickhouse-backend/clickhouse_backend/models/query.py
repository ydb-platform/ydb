from django.db.models import F, Q, query
from django.db.models.functions import Trunc

from clickhouse_backend.models import fields, sql


class QuerySet(query.QuerySet):
    def explain(self, *, format=None, type=None, **settings):
        """
        Runs an EXPLAIN on the SQL query this QuerySet would perform, and
        returns the results.
        https://clickhouse.com/docs/en/sql-reference/statements/explain/
        """
        return self.query.explain(using=self.db, format=format, type=type, **settings)

    def settings(self, **kwargs):
        clone = self._chain()
        if isinstance(clone.query, sql.Query):
            clone.query.setting_info.update(kwargs)
        return clone

    def prewhere(self, *args, **kwargs):
        """
        Return a new QuerySet instance with the args ANDed to the existing
        prewhere set.
        """
        self._not_support_combined_queries("prewhere")
        if (args or kwargs) and self.query.is_sliced:
            raise TypeError("Cannot prewhere a query once a slice has been taken.")
        clone = self._chain()
        clone._query.add_prewhere(Q(*args, **kwargs))
        return clone

    def datetimes(self, field_name, kind, order="ASC", tzinfo=None):
        """
        Return a list of datetime objects representing all available
        datetimes for the given field_name, scoped to 'kind'.
        """
        if kind not in ("year", "month", "week", "day", "hour", "minute", "second"):
            raise ValueError(
                "'kind' must be one of 'year', 'month', 'week', 'day', "
                "'hour', 'minute', or 'second'."
            )
        if order not in ("ASC", "DESC"):
            raise ValueError("'order' must be either 'ASC' or 'DESC'.")

        if kind in ("year", "month", "week"):
            output_field = fields.DateField()
        else:
            output_field = fields.DateTimeField()
        return (
            self.annotate(
                datetimefield=Trunc(
                    field_name,
                    kind,
                    output_field=output_field,
                    tzinfo=tzinfo,
                ),
                plain_field=F(field_name),
            )
            .values_list("datetimefield", flat=True)
            .distinct()
            .filter(plain_field__isnull=False)
            .order_by(("-" if order == "DESC" else "") + "datetimefield")
        )
