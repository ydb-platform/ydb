from typing import List
from introspection.model import ModelRepresentation
from django.core.management.base import BaseCommand

from introspection.inspector.inspector import AppInspector
from introspection.inspector import title, subtitle
from introspection.colors import colors


class Command(BaseCommand):
    help = "Inspect an application or model"

    def inspect_model_fields(self, model: ModelRepresentation) -> None:
        """
        Print model fields info
        """
        c = model.count()
        title(f"{model.name} ({c})")
        print(model.fields_info())

    def inspect_model_relations(self, model: ModelRepresentation) -> None:
        """
        Print model relations info
        """
        subtitle("Relations")
        for field in model.fields.values():
            if field.is_relation is True:
                try:
                    relfield = field._raw_field.remote_field.name  # type: ignore
                    raw = field._raw_field.related_model()  # type: ignore
                    msg = colors.yellow(field.name)
                    msg += " -> " + str(raw.__class__.__module__)
                    msg += "." + str(raw.__class__.__qualname__)
                    msg += f".{relfield}"
                    print(msg)
                except Exception:
                    print(
                        f"No related field for {field} of type {type(field._raw_field)}"
                    )

    def add_arguments(self, parser):  # type: ignore
        parser.add_argument("path", type=str)

    def handle(self, *args, **options):  # type: ignore
        path: str = options["path"]
        if path is None:
            raise AttributeError(
                "An app path or label is required: ex: django.contrib.auth or auth"
            )
        appname = path
        app = AppInspector(appname)
        model_names: List[ModelRepresentation] = []
        app.get_models()
        model_names = app.models
        print(f"App {app.name} models:")
        for model in model_names:
            self.inspect_model_fields(model)
            self.inspect_model_relations(model)
