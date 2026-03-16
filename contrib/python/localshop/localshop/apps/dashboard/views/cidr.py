from django.urls import reverse
from django.views import generic

from localshop.apps.dashboard import forms
from localshop.apps.dashboard.views.repository import RepositoryMixin

__all__ = [
    'CidrListView',
    'CidrCreateView',
    'CidrUpdateView',
    'CidrDeleteView',
]


class CidrListView(RepositoryMixin, generic.ListView):
    object_context_name = 'cidrs'
    template_name = 'dashboard/repository_settings/cidr_list.html'

    def get_queryset(self):
        return self.repository.cidr_list.all()


class CidrCreateView(RepositoryMixin, generic.CreateView):
    form_class = forms.AccessControlForm
    template_name = 'dashboard/repository_settings/cidr_form.html'

    def get_success_url(self):
        return reverse('dashboard:cidr_index', kwargs={
            'repo': self.repository.slug,
        })

    def get_queryset(self):
        return self.repository.cidr_list.all()


class CidrUpdateView(RepositoryMixin, generic.UpdateView):
    form_class = forms.AccessControlForm
    template_name = 'dashboard/repository_settings/cidr_form.html'

    def get_success_url(self):
        return reverse('dashboard:cidr_index', kwargs={
            'repo': self.repository.slug,
        })

    def get_queryset(self):
        return self.repository.cidr_list.all()


class CidrDeleteView(RepositoryMixin, generic.DeleteView):
    form_class = forms.AccessControlForm
    template_name = 'dashboard/repository_settings/cidr_confirm_delete.html'

    def get_success_url(self):
        return reverse('dashboard:cidr_index', kwargs={
            'repo': self.repository.slug,
        })

    def get_queryset(self):
        return self.repository.cidr_list.all()
