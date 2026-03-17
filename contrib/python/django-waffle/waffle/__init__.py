import django
from django.core.exceptions import ImproperlyConfigured

from waffle.utils import get_setting
from django.apps import apps as django_apps

VERSION = (3, 0, 0)
__version__ = '.'.join(map(str, VERSION))


def flag_is_active(request, flag_name, read_only=False):
    flag = get_waffle_flag_model().get(flag_name)
    return flag.is_active(request, read_only=read_only)


def switch_is_active(switch_name):
    switch = get_waffle_switch_model().get(switch_name)
    return switch.is_active()


def sample_is_active(sample_name):
    sample = get_waffle_sample_model().get(sample_name)
    return sample.is_active()


def get_waffle_flag_model():
    return get_waffle_model('FLAG_MODEL')


def get_waffle_switch_model():
    return get_waffle_model('SWITCH_MODEL')


def get_waffle_sample_model():
    return get_waffle_model('SAMPLE_MODEL')


def get_waffle_model(setting_name):
    """
    Returns the waffle Flag model that is active in this project.
    """
    default_model = {
        'FLAG_MODEL': 'waffle.Flag',
        'SWITCH_MODEL': 'waffle.Switch',
        'SAMPLE_MODEL': 'waffle.Sample',
    }

    # Add backwards compatibility by not requiring adding of model setting
    # for everyone who upgrades.  At some point it would be helpful to
    # require this to be defined explicitly, but no for now, to remove
    # pain from upgrading.
    default = default_model[setting_name]
    flag_model_name = get_setting(setting_name, default)

    try:
        return django_apps.get_model(flag_model_name)
    except ValueError:
        raise ImproperlyConfigured("WAFFLE_{} must be of the form 'app_label.model_name'".format(
            setting_name
        ))
    except LookupError:
        raise ImproperlyConfigured(
            "WAFFLE_{} refers to model '{}' that has not been installed".format(
                setting_name, flag_model_name
            )
        )
