from importlib import import_module

from django.conf import settings

# Default settings
BOOTSTRAP3_DEFAULTS = {
    "css_url": {
        "url": "https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/css/bootstrap.min.css",
        "integrity": "sha384-HSMxcRTRxnN+Bdg0JdbxYKrThecOKuH5zCYotlSAcp1+c8xmyTe9GYg1l9a69psu",
        "crossorigin": "anonymous",
    },
    "theme_url": None,
    "javascript_url": {
        "url": "https://stackpath.bootstrapcdn.com/bootstrap/3.4.1/js/bootstrap.min.js",
        "integrity": "sha384-aJ21OjlMXNL5UyIl/XNwTMqvzeRMZH2w8c5cRVpzpU8Y5bApTppSuUkhZXN0VxHd",
        "crossorigin": "anonymous",
    },
    "jquery_url": "//code.jquery.com/jquery.min.js",
    "javascript_in_head": False,
    "include_jquery": False,
    "horizontal_label_class": "col-md-3",
    "horizontal_field_class": "col-md-9",
    "set_placeholder": True,
    "required_css_class": "",
    "error_css_class": "has-error",
    "success_css_class": "has-success",
    "formset_renderers": {"default": "bootstrap3.renderers.FormsetRenderer"},
    "form_renderers": {"default": "bootstrap3.renderers.FormRenderer"},
    "field_renderers": {
        "default": "bootstrap3.renderers.FieldRenderer",
        "inline": "bootstrap3.renderers.InlineFieldRenderer",
    },
}


def get_bootstrap_setting(name, default=None):
    """Read a setting."""
    # Start with a copy of default settings
    bootstrap3 = BOOTSTRAP3_DEFAULTS.copy()

    # Override with user settings from settings.py
    bootstrap3.update(getattr(settings, "BOOTSTRAP3", {}))

    # Update use_i18n
    bootstrap3["use_i18n"] = i18n_enabled()

    return bootstrap3.get(name, default)


def jquery_url():
    """Return the full url to jQuery file to use."""
    return get_bootstrap_setting("jquery_url")


def javascript_url():
    """Return the full url to the Bootstrap JavaScript file."""
    return get_bootstrap_setting("javascript_url")


def css_url():
    """Return the full url to the Bootstrap CSS file."""
    return get_bootstrap_setting("css_url")


def theme_url():
    """Return the full url to the theme CSS file."""
    return get_bootstrap_setting("theme_url")


def i18n_enabled():
    """Return the projects i18n setting."""
    return getattr(settings, "USE_I18N", False)


def get_renderer(renderers, **kwargs):
    layout = kwargs.get("layout", "")
    path = renderers.get(layout, renderers["default"])
    mod, cls = path.rsplit(".", 1)
    return getattr(import_module(mod), cls)


def get_formset_renderer(**kwargs):
    renderers = get_bootstrap_setting("formset_renderers")
    return get_renderer(renderers, **kwargs)


def get_form_renderer(**kwargs):
    renderers = get_bootstrap_setting("form_renderers")
    return get_renderer(renderers, **kwargs)


def get_field_renderer(**kwargs):
    renderers = get_bootstrap_setting("field_renderers")
    return get_renderer(renderers, **kwargs)
