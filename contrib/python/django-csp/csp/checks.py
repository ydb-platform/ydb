from __future__ import annotations

import pprint
from collections.abc import Sequence
from importlib.metadata import version
from typing import TYPE_CHECKING, Any

from django.conf import settings
from django.core import checks
from django.core.checks import Error, register

from packaging.version import Version

from csp.constants import NONCE

if TYPE_CHECKING:
    from django.apps.config import AppConfig


OUTDATED_SETTINGS = [
    "CSP_CHILD_SRC",
    "CSP_CONNECT_SRC",
    "CSP_DEFAULT_SRC",
    "CSP_SCRIPT_SRC",
    "CSP_SCRIPT_SRC_ATTR",
    "CSP_SCRIPT_SRC_ELEM",
    "CSP_OBJECT_SRC",
    "CSP_STYLE_SRC",
    "CSP_STYLE_SRC_ATTR",
    "CSP_STYLE_SRC_ELEM",
    "CSP_FONT_SRC",
    "CSP_FRAME_SRC",
    "CSP_IMG_SRC",
    "CSP_MANIFEST_SRC",
    "CSP_MEDIA_SRC",
    "CSP_PREFETCH_SRC",
    "CSP_WORKER_SRC",
    "CSP_BASE_URI",
    "CSP_PLUGIN_TYPES",
    "CSP_SANDBOX",
    "CSP_FORM_ACTION",
    "CSP_FRAME_ANCESTORS",
    "CSP_NAVIGATE_TO",
    "CSP_REQUIRE_SRI_FOR",
    "CSP_REQUIRE_TRUSTED_TYPES_FOR",
    "CSP_TRUSTED_TYPES",
    "CSP_UPGRADE_INSECURE_REQUESTS",
    "CSP_BLOCK_ALL_MIXED_CONTENT",
    "CSP_REPORT_URI",
    "CSP_REPORT_TO",
]


def migrate_settings() -> tuple[dict[str, Any], bool]:
    # This function is used to migrate settings from the old format to the new format.
    config: dict[str, Any] = {
        "DIRECTIVES": {},
    }

    REPORT_ONLY = getattr(settings, "CSP_REPORT_ONLY", False)

    _EXCLUDE_URL_PREFIXES = getattr(settings, "CSP_EXCLUDE_URL_PREFIXES", None)
    if _EXCLUDE_URL_PREFIXES is not None:
        config["EXCLUDE_URL_PREFIXES"] = _EXCLUDE_URL_PREFIXES

    _REPORT_PERCENTAGE = getattr(settings, "CSP_REPORT_PERCENTAGE", None)
    if _REPORT_PERCENTAGE is not None:
        config["REPORT_PERCENTAGE"] = _REPORT_PERCENTAGE * 100

    include_nonce_in = getattr(settings, "CSP_INCLUDE_NONCE_IN", [])

    for setting in OUTDATED_SETTINGS:
        if hasattr(settings, setting):
            directive = setting[4:].replace("_", "-").lower()
            value = getattr(settings, setting)
            if value:
                config["DIRECTIVES"][directive] = value
            if directive in include_nonce_in:
                config["DIRECTIVES"][directive].append(NONCE)

    return config, REPORT_ONLY


@register(checks.Tags.security)
def check_django_csp_lt_4_0(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[Error]:
    check_settings = OUTDATED_SETTINGS + ["CSP_REPORT_ONLY", "CSP_EXCLUDE_URL_PREFIXES", "CSP_REPORT_PERCENTAGE"]
    if any(hasattr(settings, setting) for setting in check_settings):
        # Try to build the new config.
        config, REPORT_ONLY = migrate_settings()
        warning = (
            "You are using django-csp < 4.0 settings. Please update your settings to use the new format.\n"
            "See https://django-csp.readthedocs.io/en/latest/migration-guide.html for more information.\n\n"
            "We have attempted to build the new CSP config for you based on your current settings:\n\n"
            f"CONTENT_SECURITY_POLICY{'_REPORT_ONLY' if REPORT_ONLY else ''} = " + pprint.pformat(config, sort_dicts=True)
        )
        return [Error(warning, id="csp.E001")]

    return []


@register(checks.Tags.security)
def check_exclude_url_prefixes_is_not_string(app_configs: Sequence[AppConfig] | None, **kwargs: Any) -> list[Error]:
    """
    Check that EXCLUDE_URL_PREFIXES in settings is not a string.

    If it is a string it can lead to a security issue where the string is treated as a list of
    characters, resulting in '/' matching all paths excluding the CSP header from all responses.

    """
    # Skip check for django-csp < 4.0.
    if Version(version("django-csp")) < Version("4.0a1"):
        return []

    errors = []
    keys = (
        "CONTENT_SECURITY_POLICY",
        "CONTENT_SECURITY_POLICY_REPORT_ONLY",
    )
    for key in keys:
        config = getattr(settings, key, {})
        if isinstance(config, dict) and isinstance(config.get("EXCLUDE_URL_PREFIXES"), str):
            errors.append(
                Error(
                    f"EXCLUDE_URL_PREFIXES in {key} settings must be a list or tuple.",
                    id="csp.E002",
                )
            )

    return errors
