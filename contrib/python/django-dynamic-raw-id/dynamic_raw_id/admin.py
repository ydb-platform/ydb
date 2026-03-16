from __future__ import annotations

from typing import TYPE_CHECKING, Any

from django.contrib.admin.options import BaseModelAdmin

from .widgets import DynamicRawIDMultiIdWidget, DynamicRawIDWidget

if TYPE_CHECKING:
    from django.db.models.fields import Field as DB_Field
    from django.forms.fields import Field as FormField
    from django.http import HttpRequest


class DynamicRawIDMixin(BaseModelAdmin):
    dynamic_raw_id_fields = ()

    def formfield_for_foreignkey(
        self,
        db_field: DB_Field,
        request: HttpRequest | None = None,
        **kwargs: Any,
    ) -> FormField:
        if db_field.name in self.dynamic_raw_id_fields:
            rel = db_field.remote_field
            kwargs["widget"] = DynamicRawIDWidget(rel, self.admin_site)
            kwargs["help_text"] = ""
            return db_field.formfield(**kwargs)
        return super().formfield_for_foreignkey(db_field, request, **kwargs)

    def formfield_for_manytomany(
        self,
        db_field: DB_Field,
        request: HttpRequest | None = None,
        **kwargs: Any,
    ) -> FormField:
        if db_field.name in self.dynamic_raw_id_fields:
            rel = db_field.remote_field
            kwargs["widget"] = DynamicRawIDMultiIdWidget(rel, self.admin_site)
            kwargs["help_text"] = ""
            return db_field.formfield(**kwargs)
        return super().formfield_for_manytomany(db_field, request, **kwargs)
