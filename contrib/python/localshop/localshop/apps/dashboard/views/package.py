from django.contrib import messages
from django.urls import reverse
from django.shortcuts import redirect, get_object_or_404
from django.utils.translation import ugettext_lazy as _
from django.views import generic

from localshop.apps.dashboard import forms
from localshop.apps.dashboard.views.repository import RepositoryMixin
from localshop.apps.packages import models
from localshop.apps.packages.tasks import download_file, fetch_package
from localshop.utils import enqueue

__all__ = [
    'PackageAddView',
    'PackageMirrorFileView',
    'PackageRefreshView',
    'PackageDetailView',
]


class PackageAddView(RepositoryMixin, generic.FormView):
    form_class = forms.PackageAddForm
    require_role = ['owner']

    def form_valid(self, form):
        package_name = form.cleaned_data['name']
        messages.info(
            self.request,
            _("Retrieving package information from '%s'" % form.cleaned_data['name']))
        fetch_package.delay(self.repository.pk, package_name)

        return redirect(self.get_success_url())

    def form_invalid(self, form):
        messages.error(self.request, _("Invalid package name"))
        return redirect(self.get_success_url())

    def get_success_url(self):
        return reverse(
            'dashboard:repository_detail', kwargs={
                'slug': self.repository.slug
             })


class PackageMirrorFileView(RepositoryMixin, generic.View):
    require_role = ['owner']

    def post(self, request, repo, name):
        pk = request.POST.get('pk')
        release_file = models.ReleaseFile.objects.get(pk=pk)
        assert release_file.release.package.repository == self.repository

        messages.info(
            request, _("Mirroring %s in the background") % release_file.filename)

        download_file.delay(pk)
        return redirect(
            'dashboard:package_detail',
            repo=self.repository.slug,
            name=name)


class PackageRefreshView(RepositoryMixin, generic.View):
    def get(self, request, repo, name):
        package = get_object_or_404(self.repository.packages, name__iexact=name)
        enqueue(fetch_package, self.repository.pk, name)
        return redirect(package)


class PackageDetailView(RepositoryMixin, generic.DetailView):
    require_role = ['developer', 'owner']
    context_object_name = 'package'
    slug_url_kwarg = 'name'
    slug_field = 'name'
    template_name = 'dashboard/package_detail.html'

    def get_queryset(self):
        return self.repository.packages.prefetch_related('releases__files')

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(*args, **kwargs)
        context['release'] = self.object.last_release
        return context
