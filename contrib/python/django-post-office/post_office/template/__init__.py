from django.template.loader import get_template, select_template


def render_to_string(template_name, context=None, request=None, using=None):
    """
    Loads a template and renders it with a context. Returns a tuple containing the rendered template string
    and a list of attached images.

    template_name may be a string or a list of strings.
    """
    if isinstance(template_name, (list, tuple)):
        template = select_template(template_name, using=using)
    else:
        template = get_template(template_name, using=using)
    try:
        return template.render(context, request), template.template._attached_images
    except Exception:
        return template.render(context, request)
