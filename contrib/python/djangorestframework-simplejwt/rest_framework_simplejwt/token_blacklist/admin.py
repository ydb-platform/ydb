from datetime import datetime
from typing import Any, Optional, TypeVar

from django.contrib import admin
from django.contrib.auth.models import AbstractBaseUser
from django.db.models import QuerySet
from django.utils.translation import gettext_lazy as _
from rest_framework.request import Request

from ..models import TokenUser
from .models import BlacklistedToken, OutstandingToken

AuthUser = TypeVar("AuthUser", AbstractBaseUser, TokenUser)


class OutstandingTokenAdmin(admin.ModelAdmin):
    list_display = (
        "jti",
        "user",
        "created_at",
        "expires_at",
    )
    search_fields = (
        "user__id",
        "jti",
    )
    ordering = ("user",)

    def get_queryset(self, *args, **kwargs) -> QuerySet:
        qs = super().get_queryset(*args, **kwargs)

        return qs.select_related("user")

    # Read-only behavior defined below
    actions = None

    def get_readonly_fields(self, *args, **kwargs) -> list[Any]:
        return [f.name for f in self.model._meta.fields]

    def has_add_permission(self, *args, **kwargs) -> bool:
        return False

    def has_delete_permission(self, *args, **kwargs) -> bool:
        return False

    def has_change_permission(
        self, request: Request, obj: Optional[object] = None
    ) -> bool:
        return request.method in ["GET", "HEAD"] and super().has_change_permission(
            request, obj
        )


admin.site.register(OutstandingToken, OutstandingTokenAdmin)


class BlacklistedTokenAdmin(admin.ModelAdmin):
    list_display = (
        "token_jti",
        "token_user",
        "token_created_at",
        "token_expires_at",
        "blacklisted_at",
    )
    search_fields = (
        "token__user__id",
        "token__jti",
    )
    ordering = ("token__user",)

    def get_queryset(self, *args, **kwargs) -> QuerySet:
        qs = super().get_queryset(*args, **kwargs)

        return qs.select_related("token__user")

    def token_jti(self, obj: BlacklistedToken) -> str:
        return obj.token.jti

    token_jti.short_description = _("jti")  # type: ignore
    token_jti.admin_order_field = "token__jti"  # type: ignore

    def token_user(self, obj: BlacklistedToken) -> AuthUser:
        return obj.token.user

    token_user.short_description = _("user")  # type: ignore
    token_user.admin_order_field = "token__user"  # type: ignore

    def token_created_at(self, obj: BlacklistedToken) -> datetime:
        return obj.token.created_at

    token_created_at.short_description = _("created at")  # type: ignore
    token_created_at.admin_order_field = "token__created_at"  # type: ignore

    def token_expires_at(self, obj: BlacklistedToken) -> datetime:
        return obj.token.expires_at

    token_expires_at.short_description = _("expires at")  # type: ignore
    token_expires_at.admin_order_field = "token__expires_at"  # type: ignore


admin.site.register(BlacklistedToken, BlacklistedTokenAdmin)
