import re
from math import floor

from django import template
from django.contrib.messages import constants as message_constants
from django.template import Context
from django.utils.encoding import force_str
from django.utils.safestring import mark_safe

from ..bootstrap import css_url, get_bootstrap_setting, javascript_url, jquery_url, theme_url
from ..components import render_alert, render_icon
from ..forms import (
    render_button,
    render_field,
    render_field_and_label,
    render_form,
    render_form_errors,
    render_form_group,
    render_formset,
    render_formset_errors,
    render_label,
)
from ..utils import (
    handle_var,
    parse_token_contents,
    render_link_tag,
    render_script_tag,
    render_template_file,
    url_replace_param,
)

MESSAGE_LEVEL_CLASSES = {
    message_constants.DEBUG: "alert alert-warning",
    message_constants.INFO: "alert alert-info",
    message_constants.SUCCESS: "alert alert-success",
    message_constants.WARNING: "alert alert-warning",
    message_constants.ERROR: "alert alert-danger",
}

register = template.Library()


@register.filter
def bootstrap_setting(value):
    """
    Return value of a setting.

    Please consider this filter private for now, do not use it in your own templates.
    """
    return get_bootstrap_setting(value)


@register.filter
def bootstrap_message_classes(message):
    """Return the message classes for a message."""
    extra_tags = None
    try:
        extra_tags = message.extra_tags
    except AttributeError:
        pass
    if not extra_tags:
        extra_tags = ""
    classes = [extra_tags]
    try:
        level = message.level
    except AttributeError:
        pass
    else:
        try:
            classes.append(MESSAGE_LEVEL_CLASSES[level])
        except KeyError:
            classes.append("alert alert-danger")
    return " ".join(classes).strip()


@register.simple_tag
def bootstrap_jquery_url():
    """
    Return url to jquery resource.

    **Tag name**::

        bootstrap_jquery_url

    Return the full url to jQuery file to use

    Default value: ``//code.jquery.com/jquery.min.js``

    This value is configurable, see Settings section

    **Usage**::

        {% bootstrap_jquery_url %}

    **Example**::

        {% bootstrap_jquery_url %}
    """
    return jquery_url()


@register.simple_tag
def bootstrap_javascript_url():
    """
    Return the full url to the Bootstrap JavaScript library.

    Default value: ``None``

    This value is configurable, see Settings section

    **Tag name**::

        bootstrap_javascript_url

    **Usage**::

        {% bootstrap_javascript_url %}

    **Example**::

        {% bootstrap_javascript_url %}
    """
    return javascript_url()


@register.simple_tag
def bootstrap_css_url():
    """
    Return the full url to the Bootstrap CSS library.

    Default value: ``None``

    This value is configurable, see Settings section

    **Tag name**::

        bootstrap_css_url

    **Usage**::

        {% bootstrap_css_url %}

    **Example**::

        {% bootstrap_css_url %}
    """
    return css_url()


@register.simple_tag
def bootstrap_theme_url():
    """
    Return the full url to a Bootstrap theme CSS library.

    Default value: ``None``

    This value is configurable, see Settings section

    **Tag name**::

        bootstrap_theme_url

    **Usage**::

        {% bootstrap_theme_url %}

    **Example**::

        {% bootstrap_theme_url %}
    """
    return theme_url()


@register.simple_tag
def bootstrap_css():
    """
    Return HTML for Bootstrap CSS. Adjust url in settings.

    If no url is returned, we don't want this statement to return any HTML. This is intended behavior.

    Default value: ``None``

    This value is configurable, see Settings section

    **Tag name**::

        bootstrap_css

    **Usage**::

        {% bootstrap_css %}

    **Example**::

        {% bootstrap_css %}
    """
    rendered_urls = [render_link_tag(bootstrap_css_url())]
    if bootstrap_theme_url():
        rendered_urls.append(render_link_tag(bootstrap_theme_url()))
    return mark_safe("".join([url for url in rendered_urls]))


@register.simple_tag
def bootstrap_javascript(jquery=None):
    """
    Return HTML for Bootstrap JavaScript.

    Adjust url in settings. If no url is returned, we don't want this
    statement to return any HTML.
    This is intended behavior.

    Default value: ``None``

    This value is configurable, see Settings section

    **Tag name**::

        bootstrap_javascript

    **Parameters**:

        :jquery: Truthy to include jQuery as well as Bootstrap

    **Usage**::

        {% bootstrap_javascript %}

    **Example**::

        {% bootstrap_javascript jquery=1 %}
    """
    javascript = ""
    # See if we have to include jQuery
    if jquery is None:
        jquery = get_bootstrap_setting("include_jquery", False)
    # NOTE: No async on scripts, not mature enough. See issue #52 and #56
    if jquery:
        url = bootstrap_jquery_url()
        if url:
            javascript += render_script_tag(url)
    url = bootstrap_javascript_url()
    if url:
        javascript += render_script_tag(url)
    return mark_safe(javascript)


@register.simple_tag
def bootstrap_formset(*args, **kwargs):
    """
    Render a formset.

    **Tag name**::

        bootstrap_formset

    **Parameters**:

        formset
            The formset that is being rendered


        See bootstrap_field_ for other arguments

    **Usage**::

        {% bootstrap_formset formset %}

    **Example**::

        {% bootstrap_formset formset layout='horizontal' %}
    """
    return render_formset(*args, **kwargs)


@register.simple_tag
def bootstrap_formset_errors(*args, **kwargs):
    """
    Render formset errors.

    **Tag name**::

        bootstrap_formset_errors

    **Parameters**:

        formset
            The formset that is being rendered

        layout
            Context value that is available in the template ``bootstrap3/form_errors.html``
            as ``layout``.

    **Usage**::

        {% bootstrap_formset_errors formset %}

    **Example**::

        {% bootstrap_formset_errors formset layout='inline' %}
    """
    return render_formset_errors(*args, **kwargs)


@register.simple_tag
def bootstrap_form(*args, **kwargs):
    """
    Render a form.

    **Tag name**::

        bootstrap_form

    **Parameters**:

        form
            The form that is to be rendered

        exclude
            A list of field names (comma separated) that should not be rendered
            E.g. exclude=subject,bcc

        error_types
            This controls the types of errors that are rendered above the form.
            Choices are: "all", "field_errors", "non_field_errors" or "none". This will not
            affect the display of errors on the fields themselves.

            Default is "non_field_errors".

        See bootstrap_field_ for other arguments

    **Usage**::

        {% bootstrap_form form %}

    **Example**::

        {% bootstrap_form form layout='inline' %}
    """
    return render_form(*args, **kwargs)


@register.simple_tag
def bootstrap_form_errors(*args, **kwargs):
    """
    Render form errors.

    **Tag name**::

        bootstrap_form_errors

    **Parameters**:

        form
            The form that is to be rendered

        error_types
            Control which type of errors should be rendered.

            One of the following values:

                * ``'all'``
                * ``'field_errors'``
                * ``'non_field_errors'``

            :default: ``'non_field_errors'``

        layout
            Context value that is available in the template ``bootstrap3/form_errors.html`` as ``layout``.

    **Usage**::

        {% bootstrap_form_errors form %}

    **Example**::

        {% bootstrap_form_errors form layout='inline' %}
    """
    return render_form_errors(*args, **kwargs)


@register.simple_tag
def bootstrap_field(*args, **kwargs):
    """
    Render a field.

    **Tag name**::

        bootstrap_field

    **Parameters**:


        field
            The form field to be rendered

        layout
            If set to ``'horizontal'`` then the field and label will be rendered side-by-side, as long as there
            is no ``field_class`` set as well.

        form_group_class
            CSS class of the ``div`` that wraps the field and label.

            :default: ``'form-group'``

        field_class
            CSS class of the ``div`` that wraps the field.

        label_class
            CSS class of the ``label`` element. Will always have ``control-label`` as the last CSS class.

        show_help
            Show the field's help text, if the field has help text.

            :default: ``True``

        show_label
            Whether the show the label of the field.

            :default: ``True``

        exclude
            A list of field names that should not be rendered

        size
            Controls the size of the rendered ``div.form-group`` through the use of CSS classes.

            One of the following values:

                * ``'small'``
                * ``'medium'``
                * ``'large'``

        placeholder
            Set/overwrite the field's placeholder.

        label
            Overwrite the field's label.

        horizontal_label_class
            Class used on the label when the ``layout`` is set to ``horizontal``.

            :default: ``'col-md-3'``. Can be changed in :doc:`settings`

        horizontal_field_class
            Class used on the field when the ``layout`` is set to ``horizontal``.

            :default: ``'col-md-9'``. Can be changed in :doc:`settings`

        addon_before
            Text that should be prepended to the form field. Can also be an icon, e.g.
            ``'<span class="glyphicon glyphicon-calendar"></span>'``

            See the `Bootstrap docs <http://getbootstrap.com/components/#input-groups-basic>` for more examples.

        addon_after
            Text that should be appended to the form field. Can also be an icon, e.g.
            ``'<span class="glyphicon glyphicon-calendar"></span>'``

            See the `Bootstrap docs <http://getbootstrap.com/components/#input-groups-basic>` for more examples.

        addon_before_class
            Class used on the span when ``addon_before`` is used.

            One of the following values:

                * ``'input-group-addon'``
                * ``'input-group-btn'``

            :default: ``input-group-addon``

        addon_after_class
            Class used on the span when ``addon_after`` is used.

            One of the following values:

                * ``'input-group-addon'``
                * ``'input-group-btn'``

            :default: ``input-group-addon``

        error_css_class
            CSS class used when the field has an error

            :default: ``'has-error'``. Can be changed :doc:`settings`

        required_css_class
            CSS class used on the ``div.form-group`` to indicate a field is required

            :default: ``''``. Can be changed :doc:`settings`

        bound_css_class
            CSS class used when the field is bound

            :default: ``'has-success'``. Can be changed :doc:`settings`

    **Usage**::

        {% bootstrap_field field %}

    **Example**::

        {% bootstrap_field field show_label=False %}
    """
    return render_field(*args, **kwargs)


@register.simple_tag()
def bootstrap_label(*args, **kwargs):
    """
    Render a label.

    **Tag name**::

        bootstrap_label

    **Parameters**:

        content
            The label's text

        label_for
            The value that will be in the ``for`` attribute of the rendered ``<label>``

        label_class
            The CSS class for the rendered ``<label>``

        label_title
            The value that will be in the ``title`` attribute of the rendered ``<label>``

    **Usage**::

        {% bootstrap_label content %}

    **Example**::

        {% bootstrap_label "Email address" label_for="exampleInputEmail1" %}
    """
    return render_label(*args, **kwargs)


@register.simple_tag
def bootstrap_button(*args, **kwargs):
    """
    Render a button.

    **Tag name**::

        bootstrap_button

    **Parameters**:

        content
            The text to be displayed in the button

        button_type
            Optional field defining what type of button this is.

            Accepts one of the following values:

                * ``'submit'``
                * ``'reset'``
                * ``'button'``
                * ``'link'``
        icon
            Name of an icon to render in the button's visible content. See bootstrap_icon_ for acceptable values.

        button_class
            The class of button to use. If none is given, btn-default will be used.

        extra_classes
            Any extra CSS classes that should be added to the button.

        size
            Optional field to control the size of the button.

            Accepts one of the following values:

                * ``'xs'``
                * ``'sm'``
                * ``'small'``
                * ``'md'``
                * ``'medium'``
                * ``'lg'``
                * ``'large'``


        href
            Render the button as an ``<a>`` element. The ``href`` attribute is set with this value.

        name
            Value of the ``name`` attribute of the rendered element.

        value
            Value of the ``value`` attribute of the rendered element.

    **Usage**::

        {% bootstrap_button content %}

    **Example**::

        {% bootstrap_button "Save" button_type="submit" button_class="btn-primary" %}
    """
    return render_button(*args, **kwargs)


@register.simple_tag
def bootstrap_icon(icon, **kwargs):
    """
    Render an icon.

    **Tag name**::

        bootstrap_icon

    **Parameters**:

        icon
            Icon name. See the `Bootstrap docs <http://getbootstrap.com/components/#glyphicons>`_ for all icons.

        extra_classes
            Extra CSS classes to add to the icon HTML

        title
            A title for the icon (HTML title attribute)

    **Usage**::

        {% bootstrap_icon icon %}

    **Example**::

        {% bootstrap_icon "star" %}
    """
    return render_icon(icon, **kwargs)


@register.simple_tag
def bootstrap_alert(content, alert_type="info", dismissable=True):
    """
    Render an alert.

    **Tag name**::

        bootstrap_alert

    **Parameters**:

        content
            HTML content of alert

        alert_type
            * ``'info'``
            * ``'warning'``
            * ``'danger'``
            * ``'success'``

            :default: ``'info'``

        dismissable
            boolean, is alert dismissable

            :default: ``True``

    **Usage**::

        {% bootstrap_alert content %}

    **Example**::

        {% bootstrap_alert "Something went wrong" alert_type='danger' %}
    """
    return render_alert(content, alert_type, dismissable)


@register.tag("buttons")
def bootstrap_buttons(parser, token):
    """
    Render buttons for form.

    **Tag name**::

        buttons

    **Parameters**:

        submit
            Text for a submit button

        reset
            Text for a reset button

    **Usage**::

        {% buttons %}{% endbuttons %}

    **Example**::

        {% buttons submit='OK' reset="Cancel" %}{% endbuttons %}
    """
    kwargs = parse_token_contents(parser, token)
    kwargs["nodelist"] = parser.parse(("endbuttons",))
    parser.delete_first_token()
    return ButtonsNode(**kwargs)


class ButtonsNode(template.Node):
    def __init__(self, nodelist, args, kwargs, asvar, **kwargs2):
        self.nodelist = nodelist
        self.args = args
        self.kwargs = kwargs
        self.asvar = asvar

    def render(self, context):
        output_kwargs = {}
        for key in self.kwargs:
            output_kwargs[key] = handle_var(self.kwargs[key], context)
        buttons = []
        submit = output_kwargs.get("submit", None)
        reset = output_kwargs.get("reset", None)
        if submit:
            buttons.append(bootstrap_button(submit, "submit"))
        if reset:
            buttons.append(bootstrap_button(reset, "reset"))
        buttons = " ".join(buttons) + self.nodelist.render(context)
        output_kwargs.update({"label": None, "field": buttons})
        output = render_form_group(render_field_and_label(**output_kwargs))
        if self.asvar:
            context[self.asvar] = output
            return ""
        else:
            return output


@register.simple_tag(takes_context=True)
def bootstrap_messages(context, *args, **kwargs):
    """
    Show django.contrib.messages Messages in Bootstrap alert containers.

    In order to make the alerts dismissable (with the close button),
    we have to set the jquery parameter too when using the
    bootstrap_javascript tag.

    Uses the template ``bootstrap3/messages.html``.

    **Tag name**::

        bootstrap_messages

    **Parameters**:

        None.

    **Usage**::

        {% bootstrap_messages %}

    **Example**::

        {% bootstrap_javascript jquery=1 %}
        {% bootstrap_messages %}
    """
    # Custom template tags with takes_context=True somehow return Context objects. These
    # should be forced to dict, using Context.flatten()
    if isinstance(context, Context):
        context = context.flatten()
    context.update({"message_constants": message_constants})
    return render_template_file("bootstrap3/messages.html", context=context)


@register.inclusion_tag("bootstrap3/pagination.html")
def bootstrap_pagination(page, **kwargs):
    """
    Render pagination for a page.

    **Tag name**::

        bootstrap_pagination

    **Parameters**:

        page
            The page of results to show.

        pages_to_show
            Number of pages in total

            :default: ``11``

        url
            URL to navigate to for pagination forward and pagination back.

            :default: ``None``

        size
            Controls the size of the pagination through CSS.
            Defaults to being normal sized.

            One of the following:

                * ``'small'``
                * ``'large'``

            :default: ``None``

        extra
            Any extra page parameters.

            :default: ``None``

        parameter_name
            Name of the paging URL parameter.

            :default: ``'page'``

    **Usage**::

        {% bootstrap_pagination page %}

    **Example**::

        {% bootstrap_pagination lines url="/pagination?page=1" size="large" %}
        {% bootstrap_pagination page_obj extra=request.GET.urlencode %}
    """
    pagination_kwargs = kwargs.copy()
    pagination_kwargs["page"] = page
    return get_pagination_context(**pagination_kwargs)


@register.simple_tag
def bootstrap_url_replace_param(url, name, value):
    return url_replace_param(url, name, value)


def get_pagination_context(page, pages_to_show=11, url=None, size=None, extra=None, parameter_name="page"):
    """Generate Bootstrap pagination context from a page object."""
    pages_to_show = int(pages_to_show)
    if pages_to_show < 1:
        raise ValueError(f"Pagination pages_to_show should be a positive integer, you specified {pages_to_show}")
    num_pages = page.paginator.num_pages
    current_page = page.number
    half_page_num = int(floor(pages_to_show / 2))
    if half_page_num < 0:
        half_page_num = 0
    first_page = current_page - half_page_num
    if first_page <= 1:
        first_page = 1
    if first_page > 1:
        pages_back = first_page - half_page_num
        if pages_back < 1:
            pages_back = 1
    else:
        pages_back = None
    last_page = first_page + pages_to_show - 1
    if pages_back is None:
        last_page += 1
    if last_page > num_pages:
        last_page = num_pages
    if last_page < num_pages:
        pages_forward = last_page + half_page_num
        if pages_forward > num_pages:
            pages_forward = num_pages
    else:
        pages_forward = None
        if first_page > 1:
            first_page -= 1
        if pages_back is not None and pages_back > 1:
            pages_back -= 1
        else:
            pages_back = None
    pages_shown = []
    for i in range(first_page, last_page + 1):
        pages_shown.append(i)
        # Append proper character to url
    if url:
        # Remove existing page GET parameters
        url = force_str(url)
        url = re.sub(rf"\?{parameter_name}\=[^\&]+", "?", url)
        url = re.sub(rf"\&{parameter_name}\=[^\&]+", "", url)
        # Append proper separator
        if "?" in url:
            url += "&"
        else:
            url += "?"
            # Append extra string to url
    if extra:
        if not url:
            url = "?"
        url += force_str(extra) + "&"
    if url:
        url = url.replace("?&", "?")
    # Set CSS classes, see http://getbootstrap.com/components/#pagination
    pagination_css_classes = ["pagination"]
    if size == "small":
        pagination_css_classes.append("pagination-sm")
    elif size == "large":
        pagination_css_classes.append("pagination-lg")
        # Build context object
    return {
        "bootstrap_pagination_url": url,
        "num_pages": num_pages,
        "current_page": current_page,
        "first_page": first_page,
        "last_page": last_page,
        "pages_shown": pages_shown,
        "pages_back": pages_back,
        "pages_forward": pages_forward,
        "pagination_css_classes": " ".join(pagination_css_classes),
        "parameter_name": parameter_name,
    }
