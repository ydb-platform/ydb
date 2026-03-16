# -*- coding: utf-8 -*-

import django
from django.template import Library

if django.VERSION[:2] >= (1, 10):
    from django.templatetags.static import static as _static  # pylint: disable-all
else:
    from django.contrib.admin.templatetags.admin_static import (
        static as _static,  # pylint: disable-all
    )

register = Library()


@register.simple_tag()
def static(path):
    return _static(path)


CSS_VARIABLES = """
    :root {
      --button-bg: #79aec8;
      --button-fg: #fff;
      --border-bottom: #eaeaea;
    }
"""


@register.simple_tag()
def default_css_vars_if_needed():
    return CSS_VARIABLES if django.VERSION[:2] < (3, 2) else ""
