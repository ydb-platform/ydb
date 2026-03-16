from braces.views import UserFormKwargsMixin
from django.conf import settings
from django.contrib.auth import login as auth_login
from django.contrib.auth import REDIRECT_FIELD_NAME
from django.contrib.auth.mixins import LoginRequiredMixin
from django.contrib.sites.shortcuts import get_current_site
from django.core.exceptions import SuspiciousOperation
from django.urls import reverse
from django.http import HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, redirect, resolve_url
from django.template.response import TemplateResponse
from django.utils.http import is_safe_url
from django.views import generic
from django.views.decorators.cache import never_cache
from django.views.decorators.csrf import csrf_protect
from django.views.decorators.debug import sensitive_post_parameters

from localshop.apps.accounts import forms, models


class TeamListView(LoginRequiredMixin, generic.ListView):
    queryset = models.Team.objects.all()


class TeamCreateView(LoginRequiredMixin, generic.CreateView):
    model = models.Team
    fields = ['description', 'name']
    template_name = 'accounts/team_form.html'

    def form_valid(self, form):
        team = form.save()
        team.members.create(user=self.request.user, role='owner')
        return super().form_valid(form)

    def get_success_url(self):
        return reverse(
            'accounts:team_detail', kwargs={'pk': self.object.pk})


class TeamDetailView(LoginRequiredMixin, generic.DetailView):
    model = models.Team

    def get_context_data(self, *args, **kwargs):
        ctx = super().get_context_data(*args, **kwargs)
        ctx.update({
            'form_member_add': forms.TeamMemberAddForm(team=self.object),
        })
        return ctx


class TeamUpdateView(LoginRequiredMixin, generic.UpdateView):
    model = models.Team
    fields = ['description', 'name']
    template_name = 'accounts/team_form.html'

    def get_success_url(self):
        return reverse(
            'accounts:team_detail', kwargs={'pk': self.object.pk})


class TeamDeleteView(LoginRequiredMixin, generic.DeleteView):
    model = models.Team

    def get_success_url(self):
        return reverse('accounts:team_list')


class TeamMixin(object):
    def dispatch(self, request, pk):
        self.team = get_object_or_404(models.Team, pk=pk)
        return super().dispatch(request, pk)

    def get_form_kwargs(self, *args, **kwargs):
        kwargs = super().get_form_kwargs(*args, **kwargs)
        kwargs['team'] = self.team
        return kwargs


class TeamMemberAddView(LoginRequiredMixin, TeamMixin, generic.FormView):
    http_method_names = ['post']
    form_class = forms.TeamMemberAddForm

    def form_valid(self, form):
        form.save()
        return redirect('accounts:team_detail', pk=self.team.pk)

    def form_invalid(self, form):
        return redirect('accounts:team_detail', pk=self.team.pk)


class TeamMemberRemoveView(LoginRequiredMixin, TeamMixin, generic.FormView):
    http_method_names = ['post']
    form_class = forms.TeamMemberRemoveForm

    def form_valid(self, form):
        form.cleaned_data['member_obj'].delete()
        return redirect('accounts:team_detail', pk=self.team.pk)

    def form_invalid(self, form):
        return redirect('accounts:team_detail', pk=self.team.pk)


class ProfileView(LoginRequiredMixin, generic.FormView):
    form_class = forms.ProfileForm
    template_name = 'accounts/profile.html'

    def form_valid(self, form):
        form.save()
        return super().form_valid(form)

    def get_form_kwargs(self, *args, **kwargs):
        kwargs = super().get_form_kwargs(*args, **kwargs)
        kwargs['instance'] = self.request.user
        return kwargs

    def get_success_url(self):
        return reverse('accounts:profile')


class AccessKeyListView(LoginRequiredMixin, generic.ListView):
    context_object_name = 'access_keys'

    def get_queryset(self):
        return self.request.user.access_keys.all()


class AccessKeyCreateView(LoginRequiredMixin, UserFormKwargsMixin,
                          generic.CreateView):
    template_name = 'accounts/accesskey_form.html'
    form_class = forms.AccessKeyForm

    def form_valid(self, form):
        form.save()
        return super().form_valid(form)

    def get_queryset(self):
        return self.request.user.access_keys.all()

    def get_success_url(self):
        return reverse('accounts:access_key_list')


class AccessKeySecretView(LoginRequiredMixin, generic.DetailView):

    def get_queryset(self):
        return self.request.user.access_keys.all()

    def get(self, request, pk):
        if not request.is_ajax():
            raise SuspiciousOperation
        key = get_object_or_404(self.request.user.access_keys, pk=pk)
        return HttpResponse(key.secret_key)


class AccessKeyUpdateView(LoginRequiredMixin, UserFormKwargsMixin,
                          generic.UpdateView):
    template_name = 'accounts/accesskey_form.html'
    form_class = forms.AccessKeyForm

    def form_valid(self, form):
        form.save()
        return super().form_valid(form)

    def get_queryset(self):
        return self.request.user.access_keys.all()

    def get_success_url(self):
        return reverse('accounts:access_key_list')


class AccessKeyDeleteView(LoginRequiredMixin, generic.DeleteView):
    template_name = 'accounts/accesskey_confirm_delete.html'
    context_object_name = 'access_key'

    def get_queryset(self):
        return self.request.user.access_keys.all()

    def get_success_url(self):
        return reverse('accounts:access_key_list')


@sensitive_post_parameters()
@csrf_protect
@never_cache
def login(request, template_name='registration/login.html',
          redirect_field_name=REDIRECT_FIELD_NAME,
          authentication_form=forms.AuthenticationForm,
          current_app=None, extra_context=None):
    """
    Displays the login form and handles the login action.

    Copy from Django source code, added abilithy to set remember_me.
    """
    redirect_to = request.POST.get(redirect_field_name,
                                   request.GET.get(redirect_field_name, ''))

    if request.method == "POST":
        form = authentication_form(request, data=request.POST)
        if form.is_valid():

            # Ensure the user-originating redirection url is safe.
            if not is_safe_url(url=redirect_to, host=request.get_host()):
                redirect_to = resolve_url(settings.LOGIN_REDIRECT_URL)

            if not form.cleaned_data['remember_me']:
                request.session.set_expiry(0)

            # Okay, security check complete. Log the user in.
            auth_login(request, form.get_user())

            return HttpResponseRedirect(redirect_to)
    else:
        form = authentication_form(request)

    current_site = get_current_site(request)

    context = {
        'form': form,
        redirect_field_name: redirect_to,
        'site': current_site,
        'site_name': current_site.name,
    }
    if extra_context is not None:
        context.update(extra_context)

    if current_app is not None:
        request.current_app = current_app

    return TemplateResponse(request, template_name, context)
