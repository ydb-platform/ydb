from braces.forms import UserKwargModelFormMixin
from django import forms
from django.contrib.auth import get_user_model
from django.contrib.auth.forms import (
    AuthenticationForm as BaseAuthenticationForm)

from localshop.apps.accounts import models


class AuthenticationForm(BaseAuthenticationForm):
    remember_me = forms.BooleanField(required=False)


class ProfileForm(forms.ModelForm):
    class Meta:
        model = get_user_model()
        fields = ['username', 'first_name', 'last_name', 'email']


class AccessKeyForm(UserKwargModelFormMixin, forms.ModelForm):

    class Meta:
        model = models.AccessKey
        fields = ['comment']

    def save(self):
        self.instance.user = self.user
        return super().save(commit=True)


class TeamFormMixin(object):
    def __init__(self, *args, **kwargs):
        self.team = kwargs.pop('team')
        super().__init__(*args, **kwargs)


class TeamMemberAddForm(TeamFormMixin, forms.ModelForm):
    class Meta:
        model = models.TeamMember
        fields = ['user', 'role']

    def clean_user(self):
        user = self.cleaned_data['user']
        if user and self.team.members.filter(user=user).exists():
            raise forms.ValidationError(
                "%s is already a member of the team" % user.username)
        return user

    def save(self, commit=True):
        instance = super().save(commit=False)
        if commit:
            instance.team = self.team
            instance.save()
        return instance


class TeamMemberRemoveForm(TeamFormMixin, forms.Form):
    member_obj = forms.ModelChoiceField(models.TeamMember.objects.all())

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.fields['member_obj'] = forms.ModelChoiceField(
            self.team.members.all())

    def clean(self):
        member_obj = self.cleaned_data.get('member_obj')

        # This should never happen
        if not member_obj or member_obj.team.pk != self.team.pk:
            raise forms.ValidationError("Member is not part of the team")

        return self.cleaned_data
