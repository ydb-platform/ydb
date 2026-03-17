import re
import os.path as path
from importlib import import_module

from django.template.context import BaseContext


def dict_from_context(context):
    """
    Converts context to native python dict.
    """

    if isinstance(context, BaseContext):
        new_dict = {}
        for i in reversed(list(context)):
            new_dict.update(dict_from_context(i))
        return new_dict

    return dict(context)


def _iter_templatetags_modules_list():
    """
    Get list of modules that contains templatetags
    submodule.
    """
    from django.apps import apps
    all_modules = [x.name for x in apps.get_app_configs()]

    for app_path in all_modules:
        try:
            mod = import_module(app_path + ".templatetags")
            # Empty folders can lead to unexpected behavior with Python 3.
            # We make sure to have the `__file__` attribute.
            if getattr(mod, '__file__', None) is not None:
                yield (app_path, path.dirname(mod.__file__))
        except ImportError:
            pass


def patch_django_for_autoescape():
    """
    Patch django modules for make them compatible with
    jinja autoescape implementation.
    """
    from django.utils import safestring
    from django.forms.boundfield import BoundField
    from django.forms.utils import ErrorList
    from django.forms.utils import ErrorDict

    if hasattr(safestring, "SafeText"):
        if not hasattr(safestring.SafeText, "__html__"):
            safestring.SafeText.__html__ = lambda self: str(self)

    if hasattr(safestring, "SafeString"):
        if not hasattr(safestring.SafeString, "__html__"):
            safestring.SafeString.__html__ = lambda self: str(self)

    if hasattr(safestring, "SafeUnicode"):
        if not hasattr(safestring.SafeUnicode, "__html__"):
            safestring.SafeUnicode.__html__ = lambda self: str(self)

    if hasattr(safestring, "SafeBytes"):
        if not hasattr(safestring.SafeBytes, "__html__"):
            safestring.SafeBytes.__html__ = lambda self: str(self)

    if not hasattr(BoundField, "__html__"):
        BoundField.__html__ = lambda self: str(self)

    if not hasattr(ErrorList, "__html__"):
        ErrorList.__html__ = lambda self: str(self)

    if not hasattr(ErrorDict, "__html__"):
        ErrorDict.__html__ = lambda self: str(self)


def get_match_extension(using=None):
    """
    Gets the extension that the template loader will match for
    django-jinja. This returns Jinja2.match_extension.

    The "using" parameter selects with Jinja2 backend to use if
    you have multiple ones configured in settings.TEMPLATES.
    If it is None and only one Jinja2 backend is defined then it
    will use that, otherwise an ImproperlyConfigured exception
    is thrown.
    """
    from .backend import Jinja2
    from django.template import engines

    if using is None:
        engine = Jinja2.get_default()
    else:
        engine = engines[using]

    return engine.match_extension


def match_template(template_name, extension, regex):
    if extension:
        matches_extension = template_name.endswith(extension)
        if regex:
            return matches_extension and re.match(regex, template_name)
        else:
            return matches_extension
    elif regex:
        return re.match(regex, template_name)
    else:
        return True
