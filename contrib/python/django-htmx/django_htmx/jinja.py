from __future__ import annotations

from django.conf import settings
from django.templatetags.static import static
from django.utils.html import format_html
from django.utils.safestring import SafeString, mark_safe


def htmx_script(*, minified: bool = True, nonce: str | None = None) -> SafeString:
    path = f"django_htmx/htmx{'.min' if minified else ''}.js"
    if nonce is not None:
        result = format_html(
            '<script src="{}" defer nonce="{}"></script>',
            static(path),
            nonce,
        )
    else:
        result = format_html(
            '<script src="{}" defer></script>',
            static(path),
        )
    if settings.DEBUG:
        result += django_htmx_script(nonce=nonce)
    return result


def django_htmx_script(*, nonce: str | None = None) -> SafeString:
    # Optimization: whilst the script has no behaviour outside of debug mode,
    # don't include it.
    if not settings.DEBUG:
        return mark_safe("")
    if nonce is not None:
        return format_html(
            '<script src="{}" data-debug="{}" defer nonce="{}"></script>',
            static("django_htmx/django-htmx.js"),
            str(bool(settings.DEBUG)),
            nonce,
        )
    else:
        return format_html(
            '<script src="{}" data-debug="{}" defer></script>',
            static("django_htmx/django-htmx.js"),
            str(bool(settings.DEBUG)),
        )
