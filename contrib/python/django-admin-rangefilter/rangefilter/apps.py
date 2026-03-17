# -*- coding: utf-8 -*-

import django
from django.apps import AppConfig

if django.VERSION >= (2, 0, 0):
    from django.utils.translation import gettext_lazy as _
else:
    from django.utils.translation import ugettext_lazy as _  # pylint: disable=E0611


class RangeFilterConfig(AppConfig):
    name = "rangefilter"
    verbose_name = _("Range Filter")
