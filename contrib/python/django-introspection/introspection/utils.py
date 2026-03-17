from django.apps.config import AppConfig
from django.apps import apps as APPS


def get_app_config(app_name_or_label: str) -> AppConfig:
    """Get the AppConfig from an app name or label

    :param app_name_or_label: an app path or label
    :type app_name_or_label: str
    :return: the AppConfig if found
    :rtype: Tuple[AppConfig, bool]
    """
    for app in APPS.get_app_configs():  # type: ignore
        if app.name == app_name_or_label or app.label == app_name_or_label:
            return app  # type: ignore
    raise ModuleNotFoundError(f"App {app_name_or_label} was not found in settings")
