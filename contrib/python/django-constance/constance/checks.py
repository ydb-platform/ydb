from typing import Tuple, Set, List
from django.core import checks
from django.core.checks import CheckMessage
from django.utils.translation import gettext_lazy as _


@checks.register("constance")
def check_fieldsets(*args, **kwargs) -> List[CheckMessage]:
    """
    A Django system check to make sure that, if defined,
    CONFIG_FIELDSETS is consistent with settings.CONFIG.
    """
    from . import settings

    errors = []

    if hasattr(settings, "CONFIG_FIELDSETS") and settings.CONFIG_FIELDSETS:
        missing_keys, extra_keys = get_inconsistent_fieldnames()
        if missing_keys:
            check = checks.Warning(
                _(
                    "CONSTANCE_CONFIG_FIELDSETS is missing "
                    "field(s) that exists in CONSTANCE_CONFIG."
                ),
                hint=", ".join(sorted(missing_keys)),
                obj="settings.CONSTANCE_CONFIG",
                id="constance.E001",
            )
            errors.append(check)
        if extra_keys:
            check = checks.Warning(
                _(
                    "CONSTANCE_CONFIG_FIELDSETS contains extra "
                    "field(s) that does not exist in CONFIG."
                ),
                hint=", ".join(sorted(extra_keys)),
                obj="settings.CONSTANCE_CONFIG",
                id="constance.E002",
            )
            errors.append(check)
    return errors


def get_inconsistent_fieldnames() -> Tuple[Set, Set]:
    """
    Returns a pair of values:
    1) set of keys from settings.CONFIG that are not accounted for in settings.CONFIG_FIELDSETS
    2) set of keys from settings.CONFIG_FIELDSETS that are not present in settings.CONFIG
    If there are no fieldnames in settings.CONFIG_FIELDSETS, returns an empty set.
    """
    from . import settings

    if isinstance(settings.CONFIG_FIELDSETS, dict):
        fieldset_items = settings.CONFIG_FIELDSETS.items()
    else:
        fieldset_items = settings.CONFIG_FIELDSETS

    unique_field_names = set()
    for fieldset_title, fields_list in fieldset_items:
        # fields_list can be a dictionary, when a fieldset is defined as collapsible
        # https://django-constance.readthedocs.io/en/latest/#fieldsets-collapsing
        if isinstance(fields_list, dict) and 'fields' in fields_list:
            fields_list = fields_list['fields']
        unique_field_names.update(fields_list)
    if not unique_field_names:
        return unique_field_names, unique_field_names
    config_keys = set(settings.CONFIG.keys())
    missing_keys = config_keys - unique_field_names
    extra_keys = unique_field_names - config_keys
    return missing_keys, extra_keys
