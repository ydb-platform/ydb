from django.db import models


class Func(models.Func):
    @property
    def function(self):
        return self.__class__.__name__

    def deconstruct(self):
        module_name = self.__module__
        name = self.__class__.__name__
        if module_name.startswith("clickhouse_backend.models.functions"):
            module_name = "clickhouse_backend.models"
        return (
            f"{module_name}.{name}",
            self._constructor_args[0],
            self._constructor_args[1],
        )
