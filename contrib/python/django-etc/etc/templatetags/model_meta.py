from django import template

register = template.Library()


@register.simple_tag
def model_meta_verbose_name(model):
    """Returns model verbose name singular.

    Example:

        {% load model_meta %}
        {% model_meta_verbose_name my_model %}

    :param Model model:
    """
    return model._meta.verbose_name


@register.simple_tag
def model_meta_verbose_name_plural(model):
    """Returns model verbose name plural.

    Example:

        {% load model_meta %}
        {% model_meta_verbose_name_plural my_model %}

    :param Model model:
    :return:
    """
    return model._meta.verbose_name_plural
