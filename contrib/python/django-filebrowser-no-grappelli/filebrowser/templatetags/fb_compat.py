# coding: utf-8

from django import VERSION as DJANGO_VERSION
from django import template
from django.templatetags.static import static


register = template.Library()

def static_jquery():
    if DJANGO_VERSION < (1, 9):
        return static("admin/js/jquery.min.js")

    return static("admin/js/vendor/jquery/jquery.min.js")

register.simple_tag(static_jquery)


def static_search_icon():
    if DJANGO_VERSION < (1, 9):
        return static("admin/img/icon_searchbox.png")

    return static("admin/img/search.svg")

register.simple_tag(static_search_icon)
