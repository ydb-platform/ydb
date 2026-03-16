from django.views.generic.edit import CreateView as _django_CreateView
from django.views.generic.edit import DeleteView as _django_DeleteView
from django.views.generic.edit import UpdateView as _django_UpdateView

from .base import Jinja2TemplateResponseMixin


class CreateView(Jinja2TemplateResponseMixin, _django_CreateView):
    pass

class DeleteView(Jinja2TemplateResponseMixin, _django_DeleteView):
    pass

class UpdateView(Jinja2TemplateResponseMixin, _django_UpdateView):
    pass
