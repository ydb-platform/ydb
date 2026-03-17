from django.contrib.auth.mixins import AccessMixin, PermissionRequiredMixin
from django.urls import reverse
from django.http import HttpResponseForbidden
from django.shortcuts import get_object_or_404, redirect
from django.utils.functional import cached_property
from django.views import generic

from localshop.apps.dashboard import forms
from localshop.apps.packages import models, tasks

__all__ = [
    'RepositoryCreateView',
    'RepositoryDetailView',
    'RepositoryUpdateView',
    'RepositoryDeleteView',
    'RepositoryRefreshView',
]


class RepositoryCreateView(PermissionRequiredMixin, generic.CreateView):
    form_class = forms.RepositoryForm
    template_name = 'dashboard/repository_create.html'
    permission_required = 'packages.add_repository'

    def get_success_url(self):
        return reverse(
            'dashboard:repository_detail', kwargs={'slug': self.object.slug})


class RepositoryMixin(AccessMixin):
    """
    Mixin for repository specific views

    This mixin is also responsible for checking the permissions of the user
    to the given repository.
    """
    require_role = ['owner']
    repository_slug_name = 'repo'

    def dispatch(self, request, *args, **kwargs):
        if not request.user.is_authenticated:
            return self.handle_no_permission()

        if self.require_role:
            if not self.repository.check_user_role(
                request.user, self.require_role
            ):
                return HttpResponseForbidden('No access')

        return super().dispatch(request, *args, **kwargs)

    def get_form_kwargs(self, *args, **kwargs):
        kwargs = super().get_form_kwargs(*args, **kwargs)
        kwargs['repository'] = self.repository
        return kwargs

    @cached_property
    def repository(self):
        name = self.kwargs[self.repository_slug_name]
        return get_object_or_404(models.Repository.objects, slug=name)


class RepositoryDetailView(RepositoryMixin, generic.DetailView):
    model = models.Repository
    permission_required = 'packages.add_repository'
    repository_slug_name = 'slug'
    require_role = ['owner', 'developer']
    template_name = 'dashboard/repository_detail.html'

    def get_context_data(self, *args, **kwargs):
        ctx = super().get_context_data(*args, **kwargs)

        ctx.update({
            'simple_index_url': self.request.build_absolute_uri(
                self.object.simple_index_url),
        })
        return ctx


class RepositoryUpdateView(RepositoryMixin, generic.UpdateView):
    context_object_name = 'repository'
    form_class = forms.RepositoryForm
    model = models.Repository
    repository_slug_name = 'slug'
    template_name = 'dashboard/repository_settings/edit.html'

    def get_success_url(self):
        return reverse(
            'dashboard:repository_detail', kwargs={'slug': self.object.slug})

    def get_object(self):
        return self.repository

    def get_form_kwargs(self, *args, **kwargs):
        kwargs = super().get_form_kwargs(*args, **kwargs)
        del kwargs['repository']
        return kwargs


class RepositoryDeleteView(RepositoryMixin, generic.DeleteView):
    model = models.Repository
    template_name = 'dashboard/repository_settings/delete.html'
    repository_slug_name = 'slug'

    def get_success_url(self):
        return reverse('dashboard:index')

    @property
    def repository(self):
        return self.object


class RepositoryRefreshView(RepositoryMixin, generic.View):
    model = models.Repository
    permission_required = 'packages.add_repository'
    repository_slug_name = 'slug'
    require_role = ['owner']

    def get(self, request, slug):
        if not self.repository.enable_auto_mirroring:
            return redirect('dashboard:index')
        tasks.refresh_repository.delay(self.repository.pk)
        return redirect('dashboard:index')
