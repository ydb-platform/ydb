from django.utils.safestring import mark_safe

from bootstrap3.utils import add_css_class, render_tag

from .text import text_value


def render_icon(icon, **kwargs):
    """Render a Bootstrap glyphicon icon."""
    attrs = {"class": add_css_class(f"glyphicon glyphicon-{icon}", kwargs.get("extra_classes", ""))}
    title = kwargs.get("title")
    if title:
        attrs["title"] = title
    return render_tag("span", attrs=attrs)


def render_alert(content, alert_type=None, dismissable=True):
    """Render a Bootstrap alert."""
    button = ""
    if not alert_type:
        alert_type = "info"
    css_classes = ["alert", "alert-" + text_value(alert_type)]
    if dismissable:
        css_classes.append("alert-dismissable")
        button = '<button type="button" class="close" data-dismiss="alert" aria-hidden="true">&times;</button>'
    button_placeholder = "__BUTTON__"
    return mark_safe(
        render_tag(
            "div", attrs={"class": " ".join(css_classes)}, content=mark_safe(button_placeholder) + text_value(content)
        ).replace(button_placeholder, button)
    )
