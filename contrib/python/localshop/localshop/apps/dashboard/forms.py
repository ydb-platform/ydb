from django import forms
from django.utils import timezone

from localshop.apps.accounts.models import Team
from localshop.apps.packages.models import Repository
from localshop.apps.permissions.models import CIDR, Credential


class RepositoryFormMixin(object):
    def __init__(self, *args, **kwargs):
        self.repository = kwargs.pop('repository')
        super().__init__(*args, **kwargs)


class AccessControlForm(RepositoryFormMixin, forms.ModelForm):

    class Meta:
        fields = ['label', 'cidr', 'require_credentials']
        model = CIDR

    def save(self):
        instance = super().save(commit=False)
        instance.repository = self.repository
        instance.save()
        return instance


class RepositoryForm(forms.ModelForm):
    class Meta:
        model = Repository
        fields = [
            'name', 'slug', 'description', 'enable_auto_mirroring',
            'upstream_pypi_url',
        ]


class RepositoryTeamForm(RepositoryFormMixin, forms.Form):
    delete = forms.BooleanField(widget=forms.HiddenInput, required=False)
    team = forms.ModelChoiceField(Team.objects.all())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        if not self.data:
            self.fields['team'].queryset = (
                self.fields['team'].queryset
                .exclude(
                    pk__in=self.repository.teams.values_list('pk', flat=True)))

    def save(self):
        if self.cleaned_data['delete']:
            self.repository.teams.remove(self.cleaned_data['team'])
        else:
            self.repository.teams.add(self.cleaned_data['team'])


class CredentialModelForm(RepositoryFormMixin, forms.ModelForm):
    deactivated = forms.BooleanField(required=False)

    class Meta:
        model = Credential
        fields = ('comment', 'allow_upload', 'deactivated')

    def clean_deactivated(self):
        value = self.cleaned_data['deactivated']
        return timezone.now() if value else None

    def save(self, commit=True):
        instance = super().save(commit=False)

        if commit:
            instance.repository = self.repository
            instance.save()
        return instance


class PackageAddForm(RepositoryFormMixin, forms.Form):
    name = forms.CharField(max_length=200)
