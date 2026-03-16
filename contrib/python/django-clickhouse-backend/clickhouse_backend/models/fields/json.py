from django.db.models.fields import Field, json

from clickhouse_backend.driver import JSON

from .base import FieldMixin

__all__ = ["JSONField"]


class JSONField(FieldMixin, json.JSONField):
    nullable_allowed = False

    def deconstruct(self):
        name, path, args, kwargs = super().deconstruct()
        if path.startswith("clickhouse_backend.models.json"):
            path = path.replace(
                "clickhouse_backend.models.json", "clickhouse_backend.models"
            )
        return name, path, args, kwargs

    def from_db_value(self, value, expression, connection):
        return value

    def get_prep_value(self, value):
        # django 4.1 and below dumps value as json string.
        return Field.get_prep_value(self, value)

    def get_db_prep_value(self, value, connection, prepared=False):
        value = super().get_db_prep_value(value, connection, prepared)
        # django 4.1 and below does not call connection.ops.adapt_json_value
        if not isinstance(value, JSON):
            value = JSON(value)
        return value

    def get_db_prep_save(self, value, connection):
        return value
