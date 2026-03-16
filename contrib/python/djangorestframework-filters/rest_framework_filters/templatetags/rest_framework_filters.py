from django import template

register = template.Library()


@register.filter
def label(filterset, relationship):
    f = filterset
    f = f.filters[relationship]
    return f.label
