from functools import update_wrapper

from django.http import HttpResponseRedirect, Http404
from django.shortcuts import get_object_or_404
from django.urls import reverse
from django.utils.encoding import escape_uri_path, iri_to_uri
from django.utils.translation import gettext_lazy as _
from django.template.loader import render_to_string
from django.contrib import admin
from django.contrib.admin.utils import unquote
from django.contrib.admin.options import csrf_protect_m
from django.contrib.admin.views.main import ChangeList
from django import VERSION


class BaseOrderedModelAdmin:
    """
    Functionality common to both OrderedModelAdmin and OrderedInlineMixin.
    """

    request_query_string = ""

    def _get_model_info(self):
        return {"app": self.model._meta.app_label, "model": self.model._meta.model_name}

    def _get_changelist(self, request):
        list_display = self.get_list_display(request)
        list_display_links = self.get_list_display_links(request, list_display)

        args = (
            request,
            self.model,
            list_display,
            list_display_links,
            self.list_filter,
            self.date_hierarchy,
            self.search_fields,
            self.list_select_related,
            self.list_per_page,
            self.list_max_show_all,
            self.list_editable,
            self,
        )

        if VERSION >= (2, 1):
            args = args + (self.sortable_by,)

        if VERSION >= (4, 0):
            args = args + (self.search_help_text,)

        return ChangeList(*args)

    @csrf_protect_m
    def changelist_view(self, request, extra_context=None):
        cl = self._get_changelist(request)
        self.request_query_string = cl.get_query_string()
        return super().changelist_view(request, extra_context)


class OrderedModelAdmin(BaseOrderedModelAdmin, admin.ModelAdmin):
    def get_urls(self):
        from django.urls import path

        def wrap(view):
            def wrapper(*args, **kwargs):
                return self.admin_site.admin_view(view)(*args, **kwargs)

            wrapper.model_admin = self
            return update_wrapper(wrapper, view)

        model_info = self._get_model_info()

        return [
            path(
                "<path:object_id>/move-<direction>/",
                wrap(self.move_view),
                name="{app}_{model}_change_order".format(**model_info),
            )
        ] + super().get_urls()

    def move_view(self, request, object_id, direction):
        obj = get_object_or_404(self.model, pk=unquote(object_id))

        if direction not in ("up", "down", "top", "bottom"):
            raise Http404

        getattr(obj, direction)()

        # guts from request.get_full_path(), calculating ../../ and restoring GET arguments
        mangled = "/".join(escape_uri_path(request.path).split("/")[0:-3])
        redir_path = "%s%s%s" % (
            mangled,
            "/" if not mangled.endswith("/") else "",
            ("?" + iri_to_uri(request.META.get("QUERY_STRING", "")))
            if request.META.get("QUERY_STRING", "")
            else "",
        )

        return HttpResponseRedirect(redir_path)

    def move_up_down_links(self, obj):
        model_info = self._get_model_info()
        return render_to_string(
            "ordered_model/admin/order_controls.html",
            {
                "app_label": model_info["app"],
                "model_name": model_info["model"],
                "module_name": model_info["model"],  # for backwards compatibility
                "object_id": obj.pk,
                "urls": {
                    "up": reverse(
                        "{admin_name}:{app}_{model}_change_order".format(
                            admin_name=self.admin_site.name, **model_info
                        ),
                        args=[obj.pk, "up"],
                    ),
                    "down": reverse(
                        "{admin_name}:{app}_{model}_change_order".format(
                            admin_name=self.admin_site.name, **model_info
                        ),
                        args=[obj.pk, "down"],
                    ),
                    "top": reverse(
                        "{admin_name}:{app}_{model}_change_order".format(
                            admin_name=self.admin_site.name, **model_info
                        ),
                        args=[obj.pk, "top"],
                    ),
                    "bottom": reverse(
                        "{admin_name}:{app}_{model}_change_order".format(
                            admin_name=self.admin_site.name, **model_info
                        ),
                        args=[obj.pk, "bottom"],
                    ),
                },
                "query_string": self.request_query_string,
            },
        )

    move_up_down_links.short_description = _("Move")


class OrderedInlineModelAdminMixin:
    """
    ModelAdminMixin for classes that contain OrderedInilines
    """

    def get_urls(self):
        urls = super().get_urls()
        for inline in self.inlines:
            if issubclass(inline, OrderedInlineMixin):
                urls = inline(self.model, self.admin_site).get_urls() + urls
        return urls


class OrderedInlineMixin(BaseOrderedModelAdmin):
    def _get_model_info(self):
        return dict(
            **super()._get_model_info(),
            parent_model=self.parent_model._meta.model_name,
        )

    def get_urls(self):
        from django.urls import path

        def wrap(view):
            def wrapper(*args, **kwargs):
                return self.admin_site.admin_view(view)(*args, **kwargs)

            wrapper.model_admin = self
            return update_wrapper(wrapper, view)

        model_info = self._get_model_info()
        return [
            path(
                "<path:admin_id>/{model}/<path:object_id>/move-<direction>/".format(
                    **model_info
                ),
                wrap(self.move_view),
                name="{app}_{parent_model}_{model}_change_order_inline".format(
                    **model_info
                ),
            )
        ]

    def move_view(self, request, admin_id, object_id, direction):
        obj = get_object_or_404(self.model, pk=unquote(object_id))

        if direction not in ("up", "down", "top", "bottom"):
            raise Http404

        getattr(obj, direction)()

        # guts from request.get_full_path(), calculating ../../ and restoring GET arguments
        mangled = "/".join(escape_uri_path(request.path).split("/")[0:-4] + ["change"])
        redir_path = "%s%s%s" % (
            mangled,
            "/" if not mangled.endswith("/") else "",
            ("?" + iri_to_uri(request.META.get("QUERY_STRING", "")))
            if request.META.get("QUERY_STRING", "")
            else "",
        )

        return HttpResponseRedirect(redir_path)

    def move_up_down_links(self, obj):
        if not obj.pk:
            return ""

        # Find the fields which refer to the parent model of this inline, and
        # use one of them if they aren't None.
        fields = []
        for value in obj._get_related_objects():
            # Note 'a class is considered a subclass of itself' pydocs
            if issubclass(self.parent_model, type(value)):
                if value is not None and value.pk is not None:
                    fields.append(str(value.pk))

        order_obj_name = fields[0] if len(fields) > 0 else None

        model_info = self._get_model_info()
        if not order_obj_name:
            return ""

        name = "{admin_name}:{app}_{parent_model}_{model}_change_order_inline".format(
            admin_name=self.admin_site.name, **model_info
        )

        return render_to_string(
            "ordered_model/admin/order_controls.html",
            {
                "app_label": model_info["app"],
                "model_name": model_info["model"],
                "module_name": model_info["model"],  # backwards compat
                "object_id": obj.pk,
                "urls": {
                    "up": reverse(name, args=[order_obj_name, obj.pk, "up"]),
                    "down": reverse(name, args=[order_obj_name, obj.pk, "down"]),
                    "top": reverse(name, args=[order_obj_name, obj.pk, "top"]),
                    "bottom": reverse(name, args=[order_obj_name, obj.pk, "bottom"]),
                },
                "query_string": self.request_query_string,
            },
        )

    move_up_down_links.short_description = _("Move")


class OrderedTabularInline(OrderedInlineMixin, admin.TabularInline):
    pass


class OrderedStackedInline(OrderedInlineMixin, admin.StackedInline):
    pass
