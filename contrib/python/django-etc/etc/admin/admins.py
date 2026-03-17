from django.contrib import admin
from django.contrib import messages
from django.db import models
from django.http import HttpRequest, HttpResponse, HttpResponseRedirect
from django.urls import path

if False:  # pragma: nocover
    from .models import CustomModelPage  # noqa


class EtcAdmin(admin.ModelAdmin):
    """Base etc admin."""

    def message_success(self, request: HttpRequest, msg: str):
        self.message_user(request, msg, messages.SUCCESS)

    def message_warning(self, request: HttpRequest, msg: str):
        self.message_user(request, msg, messages.WARNING)

    def message_error(self, request: HttpRequest, msg: str):
        self.message_user(request, msg, messages.ERROR)


class ReadonlyAdmin(EtcAdmin):
    """Read-only etc admin base class."""

    view_on_site: bool = False
    actions = None

    def has_add_permission(self, request: HttpRequest) -> bool:
        return False

    def has_delete_permission(self, request: HttpRequest, obj: models.Model = None) -> bool:
        return False

    def changeform_view(
        self,
        request: HttpRequest,
        object_id: int = None,
        form_url: str = '',
        extra_context: dict = None
    ) -> HttpResponse:

        extra_context = extra_context or {}
        extra_context.update({
            'show_save_and_continue': False,
            'show_save': False,
        })
        return super().changeform_view(request, object_id, extra_context=extra_context)


class CustomPageModelAdmin(ReadonlyAdmin):
    """Base for admin pages with contents based on custom models."""

    def get_urls(self) -> list:
        meta = self.model._meta
        patterns = [path(
            '',
            self.admin_site.admin_view(self.view_custom),
            name=f'{meta.app_label}_{meta.model_name}_changelist'
        )]
        return patterns

    def has_add_permission(self, request: HttpRequest) -> bool:
        return True

    def view_custom(self, request: HttpRequest) -> HttpResponse:
        context: dict = {
            'show_save_and_continue': False,
            'show_save_and_add_another': False,
            'title': self.model._meta.verbose_name,
        }
        return self._changeform_view(request, object_id=None, form_url='', extra_context=context)

    def response_add(self, request: HttpRequest, obj: 'CustomModelPage', post_url_continue=None):
        bound_response = obj.bound_response
        if bound_response:
            return bound_response

        return HttpResponseRedirect(request.path)

    def save_model(self, request: HttpRequest, obj: 'CustomModelPage', form, change):
        obj.bound_request = request
        obj.bound_admin = self
        obj.save()
