from django import template

register = template.Library()


@register.filter
def form_widget(field):
    return field.field.widget.__class__.__name__
