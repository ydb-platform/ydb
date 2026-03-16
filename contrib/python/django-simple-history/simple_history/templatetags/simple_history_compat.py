from django import template
from django.template.defaulttags import url

register = template.Library()

register.tag(url)
