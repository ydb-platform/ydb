from typing import Iterator, List, Type
from django.apps.config import AppConfig
from django.db.models import Model

from introspection.model import ModelRepresentation
from introspection.utils import get_app_config


class AppInspector:
    """
    Inspect an app
    """

    app_config: AppConfig
    models: List[ModelRepresentation] = []

    def __init__(self, app_name_or_label: str) -> None:
        """
        Create an instance from an app name
        """
        app_config: AppConfig
        try:
            app_config = get_app_config(app_name_or_label)
        except ModuleNotFoundError as e:
            raise e
        self.app_config = app_config

    @property
    def name(self) -> str:
        """
        Get the app name
        """
        return self.app_config.name  # type: ignore

    def get_models(self) -> None:
        """
        Get the app models
        """
        models_type: Iterator[Type[Model]] = self.app_config.get_models()
        for model in models_type:
            self.models.append(ModelRepresentation.from_model_type(model))

    """def _convert_appname(self, appname: str) -> str:
        ""
        Remove the dots from an app name
        ""
        name = appname
        if "." in appname:
            name = appname.split(".")[-1]
        return name"""
