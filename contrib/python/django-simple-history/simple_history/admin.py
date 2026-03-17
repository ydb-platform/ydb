from collections.abc import Sequence
from typing import Any

from django import http
from django.apps import apps as django_apps
from django.conf import settings
from django.contrib import admin
from django.contrib.admin import helpers
from django.contrib.admin.utils import unquote
from django.contrib.admin.views.main import PAGE_VAR
from django.contrib.auth import get_permission_codename, get_user_model
from django.core.exceptions import PermissionDenied
from django.core.paginator import Paginator
from django.db.models import QuerySet
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
try:
    from django.urls import re_path as url
except ImportError:
    from django.conf.urls import url
from django.utils.encoding import force_str
from django.utils.html import mark_safe
from django.utils.text import capfirst
from django.utils.translation import gettext as _

from .manager import HistoricalQuerySet, HistoryManager
from .models import HistoricalChanges
from .template_utils import HistoricalRecordContextHelper
from .utils import get_history_manager_for_model, get_history_model_for_model

SIMPLE_HISTORY_EDIT = getattr(settings, "SIMPLE_HISTORY_EDIT", False)


class SimpleHistoryAdmin(admin.ModelAdmin):
    history_list_display = []

    object_history_template = "simple_history/object_history.html"
    object_history_list_template = "simple_history/object_history_list.html"
    object_history_form_template = "simple_history/object_history_form.html"
    history_list_per_page = 100

    def get_urls(self):
        """Returns the additional urls used by the Reversion admin."""
        urls = super().get_urls()
        admin_site = self.admin_site
        opts = self.model._meta
        info = opts.app_label, opts.model_name
        history_urls = [
            url(
                "^([^/]+)/history/([^/]+)/$",
                admin_site.admin_view(self.history_form_view),
                name="%s_%s_simple_history" % info,
            )
        ]
        return history_urls + urls

    def history_view(self, request, object_id, extra_context=None):
        """The 'history' admin view for this model."""
        request.current_app = self.admin_site.name
        model = self.model
        opts = model._meta
        app_label = opts.app_label
        pk_name = opts.pk.attname
        history = getattr(model, model._meta.simple_history_manager_attribute)
        object_id = unquote(object_id)
        historical_records = self.get_history_queryset(
            request, history, pk_name, object_id
        )
        history_list_display = self.get_history_list_display(request)
        # If no history was found, see whether this object even exists.
        try:
            obj = self.get_queryset(request).get(**{pk_name: object_id})
        except model.DoesNotExist:
            try:
                obj = historical_records.latest("history_date").instance
            except historical_records.model.DoesNotExist:
                raise http.Http404

        if not self.has_view_history_or_change_history_permission(request, obj):
            raise PermissionDenied

        # Use the same pagination as in Django admin, with history_list_per_page items
        paginator = Paginator(historical_records, self.history_list_per_page)
        page_obj = paginator.get_page(request.GET.get(PAGE_VAR))
        page_range = paginator.get_elided_page_range(page_obj.number)

        # Set attribute on each historical record from admin methods
        for history_list_entry in history_list_display:
            value_for_entry = getattr(self, history_list_entry, None)
            if value_for_entry and callable(value_for_entry):
                for record in page_obj.object_list:
                    setattr(record, history_list_entry, value_for_entry(record))

        self.set_history_delta_changes(request, page_obj)

        content_type = self.content_type_model_cls.objects.get_for_model(
            get_user_model()
        )
        admin_user_view = "admin:{}_{}_change".format(
            content_type.app_label,
            content_type.model,
        )

        context = {
            "title": self.history_view_title(request, obj),
            "object_history_list_template": self.object_history_list_template,
            "page_obj": page_obj,
            "page_range": page_range,
            "page_var": PAGE_VAR,
            "pagination_required": paginator.count > self.history_list_per_page,
            "module_name": capfirst(force_str(opts.verbose_name_plural)),
            "object": obj,
            "root_path": getattr(self.admin_site, "root_path", None),
            "app_label": app_label,
            "opts": opts,
            "admin_user_view": admin_user_view,
            "history_list_display": history_list_display,
            "revert_disabled": self.revert_disabled(request, obj),
        }
        context.update(self.admin_site.each_context(request))
        context.update(extra_context or {})
        extra_kwargs = {}
        return self.render_history_view(
            request, self.object_history_template, context, **extra_kwargs
        )

    def get_history_queryset(
        self, request, history_manager: HistoryManager, pk_name: str, object_id: Any
    ) -> QuerySet:
        """
        Return a ``QuerySet`` of all historical records that should be listed in the
        ``object_history_list_template`` template.
        This is used by ``history_view()``.

        :param request:
        :param history_manager:
        :param pk_name: The name of the original model's primary key field.
        :param object_id: The primary key of the object whose history is listed.
        """
        qs: HistoricalQuerySet = history_manager.filter(**{pk_name: object_id})
        if not isinstance(history_manager.model.history_user, property):
            # Only select_related when history_user is a ForeignKey (not a property)
            qs = qs.select_related("history_user")
        # Prefetch related objects to reduce the number of DB queries when diffing
        qs = qs._select_related_history_tracked_objs()
        return qs

    def get_history_list_display(self, request) -> Sequence[str]:
        """
        Return a sequence containing the names of additional fields to be displayed on
        the object history page. These can either be fields or properties on the model
        or the history model, or methods on the admin class.
        """
        return self.history_list_display

    def get_historical_record_context_helper(
        self, request, historical_record: HistoricalChanges
    ) -> HistoricalRecordContextHelper:
        """
        Return an instance of ``HistoricalRecordContextHelper`` for formatting
        the template context for ``historical_record``.
        """
        return HistoricalRecordContextHelper(self.model, historical_record)

    def set_history_delta_changes(
        self,
        request,
        historical_records: Sequence[HistoricalChanges],
        foreign_keys_are_objs=True,
    ):
        """
        Add a ``history_delta_changes`` attribute to all historical records
        except the first (oldest) one.

        :param request:
        :param historical_records:
        :param foreign_keys_are_objs: Passed to ``diff_against()`` when calculating
               the deltas; see its docstring for details.
        """
        previous = None
        for current in historical_records:
            if previous is None:
                previous = current
                continue
            # Related objects should have been prefetched in `get_history_queryset()`
            delta = previous.diff_against(
                current, foreign_keys_are_objs=foreign_keys_are_objs
            )
            helper = self.get_historical_record_context_helper(request, previous)
            previous.history_delta_changes = helper.context_for_delta_changes(delta)

            previous = current

    def history_view_title(self, request, obj):
        if self.revert_disabled(request, obj) and not SIMPLE_HISTORY_EDIT:
            return _("View history: %s") % force_str(obj)
        else:
            return _("Change history: %s") % force_str(obj)

    def response_change(self, request, obj):
        if "_change_history" in request.POST and SIMPLE_HISTORY_EDIT:
            verbose_name = obj._meta.verbose_name

            msg = _('The %(name)s "%(obj)s" was changed successfully.') % {
                "name": force_str(verbose_name),
                "obj": force_str(obj),
            }

            self.message_user(
                request, "{} - {}".format(msg, _("You may edit it again below"))
            )

            return http.HttpResponseRedirect(request.path)
        else:
            return super().response_change(request, obj)

    def history_form_view(self, request, object_id, version_id, extra_context=None):
        request.current_app = self.admin_site.name
        original_opts = self.model._meta
        model = getattr(
            self.model, self.model._meta.simple_history_manager_attribute
        ).model
        obj = get_object_or_404(
            model, **{original_opts.pk.attname: object_id, "history_id": version_id}
        ).instance
        obj._state.adding = False

        if not self.has_view_history_or_change_history_permission(request, obj):
            raise PermissionDenied

        if SIMPLE_HISTORY_EDIT:
            change_history = True
        else:
            change_history = False

        if "_change_history" in request.POST and SIMPLE_HISTORY_EDIT:
            history = get_history_manager_for_model(obj)
            obj = history.get(pk=version_id).instance

        formsets = []
        form_class = self.get_form(request, obj)
        if request.method == "POST":
            form = form_class(request.POST, request.FILES, instance=obj)
            if form.is_valid():
                new_object = self.save_form(request, form, change=True)
                self.save_model(request, new_object, form, change=True)
                form.save_m2m()

                self.log_change(
                    request,
                    new_object,
                    self.construct_change_message(request, form, formsets),
                )
                return self.response_change(request, new_object)

        else:
            form = form_class(instance=obj)

        admin_form = helpers.AdminForm(
            form,
            self.get_fieldsets(request, obj),
            self.prepopulated_fields,
            self.get_readonly_fields(request, obj),
            model_admin=self,
        )

        model_name = original_opts.model_name
        url_triplet = self.admin_site.name, original_opts.app_label, model_name
        context = {
            "title": self.history_form_view_title(request, obj),
            "adminform": admin_form,
            "object_id": object_id,
            "original": obj,
            "is_popup": False,
            "media": mark_safe(self.media + admin_form.media),
            "errors": helpers.AdminErrorList(form, formsets),
            "app_label": original_opts.app_label,
            "original_opts": original_opts,
            "changelist_url": reverse("%s:%s_%s_changelist" % url_triplet),
            "change_url": reverse("%s:%s_%s_change" % url_triplet, args=(obj.pk,)),
            "history_url": reverse("%s:%s_%s_history" % url_triplet, args=(obj.pk,)),
            "change_history": change_history,
            "revert_disabled": self.revert_disabled(request, obj),
            # Context variables copied from render_change_form
            "add": False,
            "change": True,
            "has_add_permission": self.has_add_permission(request),
            "has_view_permission": self.has_view_history_permission(request, obj),
            "has_change_permission": self.has_change_history_permission(request, obj),
            "has_delete_permission": self.has_delete_permission(request, obj),
            "has_file_field": True,
            "has_absolute_url": False,
            "form_url": "",
            "opts": model._meta,
            "content_type_id": self.content_type_model_cls.objects.get_for_model(
                self.model
            ).id,
            "save_as": self.save_as,
            "save_on_top": self.save_on_top,
            "root_path": getattr(self.admin_site, "root_path", None),
        }
        context.update(self.admin_site.each_context(request))
        context.update(extra_context or {})
        extra_kwargs = {}
        return self.render_history_view(
            request, self.object_history_form_template, context, **extra_kwargs
        )

    def history_form_view_title(self, request, obj):
        if self.revert_disabled(request, obj):
            return _("View %s") % force_str(obj)
        else:
            return _("Revert %s") % force_str(obj)

    def render_history_view(self, request, template, context, **kwargs):
        """Catch call to render, to allow overriding."""
        return render(request, template, context, **kwargs)

    def save_model(self, request, obj, form, change):
        """Set special model attribute to user for reference after save"""
        obj._history_user = request.user
        super().save_model(request, obj, form, change)

    @property
    def content_type_model_cls(self):
        """Returns the ContentType model class."""
        return django_apps.get_model("contenttypes.contenttype")

    def revert_disabled(self, request, obj=None):
        """If `True`, hides the "Revert" button in the `submit_line.html` template."""
        if getattr(settings, "SIMPLE_HISTORY_REVERT_DISABLED", False):
            return True
        elif self.has_view_history_permission(
            request, obj
        ) and not self.has_change_history_permission(request, obj):
            return True
        return False

    def has_view_permission(self, request, obj=None):
        return super().has_view_permission(request, obj)

    def has_change_permission(self, request, obj=None):
        return super().has_change_permission(request, obj)

    def has_view_or_change_permission(self, request, obj=None):
        return self.has_view_permission(request, obj) or self.has_change_permission(
            request, obj
        )

    def has_view_history_or_change_history_permission(self, request, obj=None):
        if self.enforce_history_permissions:
            return self.has_view_history_permission(
                request, obj
            ) or self.has_change_history_permission(request, obj)
        return self.has_view_or_change_permission(request, obj)

    def has_view_history_permission(self, request, obj=None):
        if self.enforce_history_permissions:
            opts_history = get_history_model_for_model(self.model)._meta
            codename_view_history = get_permission_codename("view", opts_history)
            return request.user.has_perm(
                f"{opts_history.app_label}.{codename_view_history}"
            )
        return self.has_view_permission(request, obj)

    def has_change_history_permission(self, request, obj=None):
        if self.enforce_history_permissions:
            opts_history = get_history_model_for_model(self.model)._meta
            codename_change_history = get_permission_codename("change", opts_history)
            return request.user.has_perm(
                f"{opts_history.app_label}.{codename_change_history}"
            )
        return self.has_change_permission(request, obj)

    @property
    def enforce_history_permissions(self):
        return getattr(
            settings, "SIMPLE_HISTORY_ENFORCE_HISTORY_MODEL_PERMISSIONS", False
        )
