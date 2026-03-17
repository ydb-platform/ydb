from django.views.generic.detail import DetailView as _django_DetailView

from .base import Jinja2TemplateResponseMixin


class DetailView(Jinja2TemplateResponseMixin, _django_DetailView):
    pass
