from django.contrib.admin.widgets import AdminFileWidget
from django.forms import (
    CheckboxSelectMultiple,
    EmailInput,
    FileInput,
    HiddenInput,
    NumberInput,
    PasswordInput,
    Textarea,
    TextInput,
    URLInput,
)
from django.forms.widgets import CheckboxInput
from django.utils.safestring import mark_safe

from .bootstrap import (
    get_bootstrap_setting,
    get_field_renderer,
    get_form_renderer,
    get_formset_renderer,
)
from .components import render_icon
from .exceptions import BootstrapError
from .text import text_concat, text_value
from .utils import add_css_class, render_tag

FORM_GROUP_CLASS = "form-group"

WIDGETS_NO_REQUIRED = (
    AdminFileWidget,
    HiddenInput,
    FileInput,
    CheckboxInput,
    CheckboxSelectMultiple,
)


def render_formset(formset, **kwargs):
    """Render a formset to a Bootstrap layout."""
    renderer_cls = get_formset_renderer(**kwargs)
    return renderer_cls(formset, **kwargs).render()


def render_formset_errors(formset, **kwargs):
    """Render formset errors to a Bootstrap layout."""
    renderer_cls = get_formset_renderer(**kwargs)
    return renderer_cls(formset, **kwargs).render_errors()


def render_form(form, **kwargs):
    """Render a form to a Bootstrap layout."""
    renderer_cls = get_form_renderer(**kwargs)
    return renderer_cls(form, **kwargs).render()


def render_form_errors(form, error_types="non_field_errors", **kwargs):
    """Render form errors to a Bootstrap layout."""
    renderer_cls = get_form_renderer(**kwargs)
    return renderer_cls(form, **kwargs).render_errors(error_types)


def render_field(field, **kwargs):
    """Render a field to a Bootstrap layout."""
    renderer_cls = get_field_renderer(**kwargs)
    return renderer_cls(field, **kwargs).render()


def render_label(content, label_for=None, label_class=None, label_title=""):
    """Render a label with content."""
    attrs = {}
    if label_for:
        attrs["for"] = label_for
    if label_class:
        attrs["class"] = label_class
    if label_title:
        attrs["title"] = label_title
    return render_tag("label", attrs=attrs, content=content)


def render_button(
    content,
    button_type=None,
    icon=None,
    button_class="btn-default",
    size="",
    href="",
    name=None,
    value=None,
    title=None,
    extra_classes="",
    id="",
):
    """Render a button with content."""
    attrs = {}
    classes = add_css_class("btn", button_class)
    size = text_value(size).lower().strip()
    if size == "xs":
        classes = add_css_class(classes, "btn-xs")
    elif size == "sm" or size == "small":
        classes = add_css_class(classes, "btn-sm")
    elif size == "lg" or size == "large":
        classes = add_css_class(classes, "btn-lg")
    elif size == "md" or size == "medium":
        pass
    elif size:
        raise BootstrapError(f'Parameter "size" should be "xs", "sm", "lg" or empty ("{size}" given).')
    if button_type:
        if button_type not in ("submit", "reset", "button", "link"):
            raise BootstrapError(
                'Parameter "button_type" should be "submit", "reset", "button", "link" or empty '
                + f'("{button_type}" given).'
            )
        attrs["type"] = button_type
    classes = add_css_class(classes, extra_classes)
    attrs["class"] = classes
    icon_content = render_icon(icon) if icon else ""
    if href:
        attrs["href"] = href
        tag = "a"
    else:
        tag = "button"
    if id:
        attrs["id"] = id
    if name:
        attrs["name"] = name
    if value:
        attrs["value"] = value
    if title:
        attrs["title"] = title
    return render_tag(
        tag,
        attrs=attrs,
        content=mark_safe(text_concat(icon_content, content, separator=" ")),
    )


def render_field_and_label(field, label, field_class="", label_for=None, label_class="", layout="", **kwargs):
    """Render a field with its label."""
    if layout == "horizontal":
        if not label_class:
            label_class = get_bootstrap_setting("horizontal_label_class")
        if not field_class:
            field_class = get_bootstrap_setting("horizontal_field_class")
        if not label:
            label = mark_safe("&#160;")
        label_class = add_css_class(label_class, "control-label")
    html = field
    if field_class:
        html = f'<div class="{field_class}">{html}</div>'
    if label:
        html = render_label(label, label_for=label_for, label_class=label_class) + html
    return html


def render_form_group(content, css_class=FORM_GROUP_CLASS):
    """Render a Bootstrap form group."""
    return f'<div class="{css_class}">{content}</div>'


def is_widget_required_attribute(widget):
    """Return whether this widget is required."""
    if not widget.is_required:
        return False
    if isinstance(widget, WIDGETS_NO_REQUIRED):
        return False
    return True


def is_widget_with_placeholder(widget):
    """
    Return whether this a widget that should have a placeholder.

    Only text, search, url, tel, e-mail, password, number have placeholders
    """
    return isinstance(widget, (TextInput, Textarea, NumberInput, EmailInput, URLInput, PasswordInput))
