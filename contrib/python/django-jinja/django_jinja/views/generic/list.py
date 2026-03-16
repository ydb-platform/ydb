from django.views.generic.list import ListView as _django_ListView

from .base import Jinja2TemplateResponseMixin


class ListView(Jinja2TemplateResponseMixin, _django_ListView):
    pass
