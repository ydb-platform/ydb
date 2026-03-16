"""dynamic_raw_id filters."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django import forms
from django.contrib import admin

from .widgets import DynamicRawIDWidget

if TYPE_CHECKING:
    from django.db.models import Field as DbField
    from django.db.models import Model, QuerySet
    from django.forms import Form
    from django.http import HttpRequest


class DynamicRawIDFilterForm(forms.Form):
    """Form for dynamic_raw_id filter."""

    def __init__(
        self,
        rel: DbField,
        admin_site: admin.AdminSite,
        field_name: str,
        **kwargs: Any,
    ) -> None:
        """Construct field for given field rel."""
        super().__init__(**kwargs)
        self.fields[field_name] = forms.CharField(
            label="",
            widget=DynamicRawIDWidget(rel=rel, admin_site=admin_site),
            required=False,
        )


class DynamicRawIDFilter(admin.filters.FieldListFilter):
    """Filter list queryset by primary key of a related object."""

    template = "dynamic_raw_id/admin/filters/dynamic_raw_id_filter.html"

    def __init__(  # noqa: PLR0913 Too Many arguments
        self,
        field: DbField,
        request: HttpRequest,
        params: dict[str, Any],
        model: Model,
        model_admin: admin.ModelAdmin,
        field_path: str,
    ) -> None:
        """Use GET param for lookup and form initialization."""
        self.lookup_kwarg = field_path
        super().__init__(field, request, params, model, model_admin, field_path)
        rel = field.remote_field
        self.form = self.get_form(request, rel, model_admin.admin_site)

    def choices(self, changelist: Any) -> list:
        """Filter choices do not exist, since we choose the popup value."""
        return []

    def expected_parameters(self) -> str:
        """Return GET params for this filter."""
        return self.lookup_kwarg

    def get_form(
        self,
        request: HttpRequest,
        rel: DbField,
        admin_site: admin.AdminSite,
    ) -> Form:
        """Return a filter form."""
        return DynamicRawIDFilterForm(
            admin_site=admin_site,
            rel=rel,
            field_name=self.field_path,
            data={self.field_path: request.GET.get(self.field_path, "")},
        )

    def queryset(
        self,
        request: HttpRequest,
        queryset: QuerySet,
    ) -> QuerySet:
        """Filter queryset using params from the form."""
        if self.form.is_valid():
            # get no null params
            filter_params = dict(
                filter(lambda x: bool(x[1]), self.form.cleaned_data.items())
            )
            return queryset.filter(**filter_params)
        return queryset
