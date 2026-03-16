from django.views.generic.dates import ArchiveIndexView as _django_ArchiveIndexView
from django.views.generic.dates import YearArchiveView as _django_YearArchiveView
from django.views.generic.dates import MonthArchiveView as _django_MonthArchiveView
from django.views.generic.dates import WeekArchiveView as _django_WeekArchiveView
from django.views.generic.dates import DayArchiveView as _django_DayArchiveView
from django.views.generic.dates import TodayArchiveView as _django_TodayArchiveView
from django.views.generic.dates import DateDetailView as _django_DateDetailView

from .base import Jinja2TemplateResponseMixin


class ArchiveIndexView(Jinja2TemplateResponseMixin, _django_ArchiveIndexView):
    pass

class YearArchiveView(Jinja2TemplateResponseMixin, _django_YearArchiveView):
    pass

class MonthArchiveView(Jinja2TemplateResponseMixin, _django_MonthArchiveView):
    pass

class WeekArchiveView(Jinja2TemplateResponseMixin, _django_WeekArchiveView):
    pass

class DayArchiveView(Jinja2TemplateResponseMixin, _django_DayArchiveView):
    pass

class TodayArchiveView(Jinja2TemplateResponseMixin, _django_TodayArchiveView):
    pass

class DateDetailView(Jinja2TemplateResponseMixin, _django_DateDetailView):
    pass
