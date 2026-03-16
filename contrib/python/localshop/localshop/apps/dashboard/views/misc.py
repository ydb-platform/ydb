import operator

from django.contrib.auth.mixins import LoginRequiredMixin
from django.urls import reverse
from django.views import generic

from localshop.apps.dashboard import forms
from localshop.apps.dashboard.views.repository import RepositoryMixin
from localshop.apps.packages import models


class IndexView(LoginRequiredMixin, generic.TemplateView):
    template_name = 'dashboard/index.html'

    def get_context_data(self):
        return {
            'repositories': self.repositories,
        }

    @property
    def repositories(self):
        user = self.request.user

        if user.is_superuser:
            return models.Repository.objects.all()

        repositories = set()
        for team_membership in user.team_memberships.all():
            for repository in team_membership.team.repositories.all():
                repositories.add(repository)
        return sorted(repositories, key=operator.attrgetter('name'))


class TeamAccessView(RepositoryMixin, generic.FormView):
    form_class = forms.RepositoryTeamForm
    template_name = 'dashboard/repository_settings/teams.html'

    def get_success_url(self):
        return reverse('dashboard:team_access', kwargs={
            'repo': self.repository.slug,
        })

    def form_valid(self, form):
        form.save()
        return super().form_valid(form)
