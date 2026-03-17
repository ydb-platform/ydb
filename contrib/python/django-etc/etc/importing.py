from types import ModuleType
from typing import Optional, List

from django.utils.module_loading import module_has_submodule

try:
    from django.utils.module_loading import import_module

except ImportError:
    # Django <=1.9.0
    from django.utils.importlib import import_module


def import_app_module(app_name: str, module_name: str) -> Optional[ModuleType]:
    """Returns a module from a given app by its name.

    :param app_name:
    :param module_name:

    """
    name_split = app_name.split('.')
    if name_split[-1][0].isupper():  # Seems that we have app config class path here.
        app_name = '.'.join(name_split[:-2])

    module = import_module(app_name)

    try:
        sub_module = import_module(f'{app_name}.{module_name}')
        return sub_module

    except:

        # The same bubbling strategy as in autodiscover_modules().
        if module_has_submodule(module, module_name):  # Module is in a package.
            raise

        return None


def import_project_modules(module_name: str) -> List[ModuleType]:
    """Imports modules from registered apps using given module name
    and returns them as a list.

    :param module_name:

    """
    from django.conf import settings

    submodules = []
    for app in settings.INSTALLED_APPS:
        module = import_app_module(app, module_name)
        if module is not None:
            submodules.append(module)

    return submodules
