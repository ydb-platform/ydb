from django.db import models

from .json import patch_jsonfield

__all__ = [
    "patch_fields",
    "patch_auto_field",
    "patch_jsonfield",
]


def patch_fields():
    patch_auto_field()
    patch_jsonfield()


def patch_auto_field():
    def rel_db_type_decorator(cls):
        old_func = cls.rel_db_type

        def rel_db_type(self, connection):
            if connection.vendor == "clickhouse":
                return self.db_type(connection)
            return old_func(self, connection)

        cls.rel_db_type = rel_db_type
        return cls

    rel_db_type_decorator(models.AutoField)
    rel_db_type_decorator(models.SmallAutoField)
    rel_db_type_decorator(models.BigAutoField)
