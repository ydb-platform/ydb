from django.contrib.sites.models import Site
from django.core.exceptions import SuspiciousOperation
from django.urls import reverse
from django.http import HttpResponse
from django.shortcuts import get_object_or_404
from django.views import generic

from localshop.apps.dashboard import forms
from localshop.apps.dashboard.views.repository import RepositoryMixin

__all__ = [
    'CredentialListView',
    'CredentialCreateView',
    'CredentialSecretKeyView',
    'CredentialUpdateView',
    'CredentialDeleteView',
]


class CredentialListView(RepositoryMixin, generic.ListView):
    object_context_name = 'credentials'
    template_name = 'dashboard/repository_settings/credential_list.html'

    def get_queryset(self):
        return self.repository.credentials.all()

    def get_context_data(self, **kwargs):
        context = super().get_context_data(**kwargs)
        context['current_url'] = Site.objects.get_current()
        return context


class CredentialCreateView(RepositoryMixin, generic.CreateView):
    form_class = forms.CredentialModelForm
    template_name = 'dashboard/repository_settings/credential_form.html'

    def get_queryset(self):
        return self.repository.credentials.all()

    def get_success_url(self):
        return reverse('dashboard:credential_index', kwargs={
            'repo': self.repository.slug,
        })


class CredentialSecretKeyView(RepositoryMixin, generic.View):

    def get(self, request, repo, access_key):
        if not request.is_ajax():
            raise SuspiciousOperation
        credential = get_object_or_404(
            self.repository.credentials, access_key=access_key)
        return HttpResponse(credential.secret_key)


class CredentialUpdateView(RepositoryMixin, generic.UpdateView):
    form_class = forms.CredentialModelForm
    slug_field = 'access_key'
    slug_url_kwarg = 'access_key'
    template_name = 'dashboard/repository_settings/credential_form.html'

    def get_queryset(self):
        return self.repository.credentials.all()

    def get_success_url(self):
        return reverse('dashboard:credential_index', kwargs={
            'repo': self.repository.slug,
        })


class CredentialDeleteView(RepositoryMixin, generic.DeleteView):
    slug_field = 'access_key'
    slug_url_kwarg = 'access_key'

    def get_success_url(self):
        return reverse('dashboard:credential_index', kwargs={
            'repo': self.repository.slug,
        })
