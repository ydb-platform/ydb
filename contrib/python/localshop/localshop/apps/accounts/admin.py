from django import forms
from django.contrib import admin
from django.contrib.auth.admin import UserAdmin
from django.contrib.auth.forms import ReadOnlyPasswordHashField
from django.contrib.auth.models import Group
from django.utils.translation import ugettext_lazy as _

from localshop.apps.accounts import models


class TeamMemberInline(admin.TabularInline):
    model = models.TeamMember
    list_display = ['user', 'role']


class UserCreationForm(forms.ModelForm):
    """
    A form for creating new users. Includes all the required
    fields, plus a repeated password.
    """

    password1 = forms.CharField(
        label='Password',
        widget=forms.PasswordInput,
    )
    password2 = forms.CharField(
        label='Password confirmation',
        widget=forms.PasswordInput,
    )

    class Meta:
        model = models.User
        fields = ['email']

    def clean_password2(self):
        # Check that the two password entries match
        password1 = self.cleaned_data.get("password1")
        password2 = self.cleaned_data.get("password2")
        if password1 and password2 and password1 != password2:
            raise forms.ValidationError("Passwords don't match")
        return password2

    def save(self, commit=True):
        # Save the provided password in hashed format
        user = super(UserCreationForm, self).save(commit=False)
        user.set_password(self.cleaned_data["password1"])
        if commit:
            user.save()
        return user


class UserChangeForm(forms.ModelForm):
    """
    A form for updating users. Includes all the fields on
    the user, but replaces the password field with admin's
    password hash display field.
    """

    password = ReadOnlyPasswordHashField(
        label=("Password"),
        help_text=_(
            "Raw passwords are not stored, so there is no way to see "
            "this user's password, but you can change the password "
            "using <a href=\"password/\">this form</a>."
        ),
    )

    class Meta:
        model = models.User
        fields = (
            'email',
            'password',
            'is_active',
            'is_staff',
            'is_superuser',
        )

    def clean_password(self):
        # Regardless of what the user provides, return the initial value.
        # This is done here, rather than on the field, because the
        # field does not have access to the initial value
        return self.initial["password"]


@admin.register(models.AccessKey)
class AccessKeyAdmin(admin.ModelAdmin):

    raw_id_fields = (
        'user',
    )

    list_display = (
        'user',
        'created',
        'last_usage',
    )


@admin.register(models.Team)
class TeamAdmin(admin.ModelAdmin):

    inlines = (
        TeamMemberInline,
    )

    list_display = (
        'name',
    )


@admin.register(models.User)
class UserAdmin(UserAdmin):

    form = UserChangeForm
    add_form = UserCreationForm

    fieldsets = (
        (None, {
            'fields': ['username', 'email', 'password'],
        }),
        ('Information', {
            'fields': ['first_name', 'last_name'],
        }),
        ('Permissions', {
            'fields': ['is_active', 'is_staff', 'is_superuser'],
        }),
    )
    add_fieldsets = (
        (None, {
            'classes': ['wide'],
            'fields': ['email', 'password1', 'password2'],
        }),
    )

    list_display = (
        'email',
        'is_active',
        'is_staff',
        'is_superuser',
    )

    list_filter = (
        'is_active',
        'is_superuser',
    )

    search_fields = (
        'email',
        'username',
    )

    ordering = (
        'email',
    )

    filter_horizontal = []


admin.site.unregister(Group)
