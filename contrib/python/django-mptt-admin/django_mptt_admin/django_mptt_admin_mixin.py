from functools import update_wrapper
from typing import Union

from django.conf import settings
from django.templatetags.static import static
from django.contrib.admin.templatetags.admin_urls import add_preserved_filters
from django.core.exceptions import PermissionDenied, SuspiciousOperation
from django.http import JsonResponse
from django.template.response import TemplateResponse
from django.contrib.admin.options import csrf_protect_m, ModelAdmin
from django.contrib.admin.utils import unquote, quote
from django.contrib.admin.options import IS_POPUP_VAR
from django.db import transaction
from django.utils.http import urlencode
from django.forms import Media
from django.urls import re_path, reverse
from django.views.i18n import JavaScriptCatalog
import django

from . import util
from .tree_change_list import TreeChangeList


class DjangoMpttAdminMixin:
    tree_animation_speed = None
    tree_auto_open = 1
    tree_load_on_demand = 1
    tree_mouse_delay = None
    trigger_save_after_move = False

    # Autoescape the tree data; default is True
    autoescape = True

    # useContextMenu option for the tree; default is False
    use_context_menu = False

    change_list_template = "django_mptt_admin/grid_view.html"
    change_tree_template = "django_mptt_admin/change_list.html"

    # define which field of the model should be the label for tree items
    item_label_field_name = None

    # list and tree filter
    list_filter = ()

    change_list_tree_class = TreeChangeList

    @csrf_protect_m
    def changelist_view(
        self: Union[ModelAdmin, "DjangoMpttAdminMixin"], request, extra_context=None
    ):
        request.current_app = self.admin_site.name
        is_popup = IS_POPUP_VAR in request.GET
        if is_popup:
            return super(DjangoMpttAdminMixin, self).changelist_view(
                request, extra_context=extra_context
            )

        if not self.has_view_or_change_permission(request):
            raise PermissionDenied()

        change_list = self.get_change_list_for_tree(request)

        preserved_filters = self.get_preserved_filters(request)

        def get_admin_url_with_filters(name):
            admin_url = self.get_admin_url(name)

            if change_list.params:
                return admin_url + change_list.get_query_string()
            else:
                return admin_url

        def get_admin_url_with_preserved_filters(name):
            return add_preserved_filters(
                {"preserved_filters": preserved_filters, "opts": self.opts},
                self.get_admin_url(name),
            )

        def get_csrf_cookie_name():
            if settings.CSRF_USE_SESSIONS:
                return ""
            else:
                return settings.CSRF_COOKIE_NAME

        grid_url = get_admin_url_with_filters("grid")
        tree_json_url = get_admin_url_with_filters("tree_json")
        insert_at_url = get_admin_url_with_preserved_filters("add")

        tree_options = {
            "autoescape": self.autoescape,
            "csrf_cookie_name": get_csrf_cookie_name(),
            "drag_and_drop": self.is_drag_and_drop_enabled(),
            "grid_url": grid_url,
            "has_add_permission": self.has_add_permission(request),
            "has_change_permission": self.has_change_permission(request),
            "insert_at_url": insert_at_url,
            "jsi18n_url": self.get_admin_url("jsi18n"),
            "model_name": util.get_model_name(self.model),
            "tree_animation_speed": self.tree_animation_speed,
            "tree_auto_open": self.tree_auto_open,
            "tree_json_url": tree_json_url,
            "tree_mouse_delay": self.get_tree_mouse_delay(),
            "use_context_menu": self.use_context_menu,
        }

        context = {
            **self.admin_site.each_context(request),
            "django_major_version": django.VERSION[0],
            "module_name": str(self.opts.verbose_name_plural),
            "title": change_list.title,
            "subtitle": None,
            "is_popup": change_list.is_popup,
            "to_field": change_list.to_field,
            "cl": change_list,
            "media": self.get_tree_media(),
            "opts": change_list.opts,
            "preserved_filters": self.get_preserved_filters(request),
            **tree_options,
            **(extra_context or {}),
        }

        request.current_app = self.admin_site.name

        return TemplateResponse(request, self.change_tree_template, context)

    def get_urls(self: Union[ModelAdmin, "DjangoMpttAdminMixin"]):
        def wrap(view, cacheable=False):
            def wrapper(*args, **kwargs):
                return self.admin_site.admin_view(view, cacheable)(*args, **kwargs)

            return update_wrapper(wrapper, view)

        def create_url(regex, url_name, view, kwargs=None, cacheable=False):
            return re_path(
                regex,
                wrap(view, cacheable),
                kwargs=kwargs,
                name="{0!s}_{1!s}_{2!s}".format(
                    self.opts.app_label,
                    util.get_model_name(self.model),
                    url_name,
                ),
            )

        def create_js_catalog_url():
            packages = ["django_mptt_admin"]
            url_pattern = r"^jsi18n/$"

            return create_url(
                url_pattern,
                "jsi18n",
                JavaScriptCatalog.as_view(packages=packages),
                cacheable=True,
            )

        # prepend new urls to existing urls
        return [
            create_url(r"^(.+)/move/$", "move", self.move_view),
            create_url(r"^tree_json/$", "tree_json", self.tree_json_view),
            create_url(r"^grid/$", "grid", self.grid_view),
            create_js_catalog_url(),
        ] + super(DjangoMpttAdminMixin, self).get_urls()

    def get_tree_media(self: ModelAdmin):
        django_mptt_admin_js = (
            "django_mptt_admin.coverage.js"
            if getattr(settings, "DJANGO_MPTT_ADMIN_COVERAGE_JS", False)
            else "django_mptt_admin.js"
        )

        js = [
            "admin/js/jquery.init.js",
            static("django_mptt_admin/jquery_namespace.js"),
            static(f"django_mptt_admin/{django_mptt_admin_js}"),
        ]
        css = dict(all=(static("django_mptt_admin/django_mptt_admin.css"),))

        tree_media = Media(js=js, css=css)

        return self.media + tree_media

    @csrf_protect_m
    @transaction.atomic()
    def move_view(self: Union[ModelAdmin, "DjangoMpttAdminMixin"], request, object_id):
        request.current_app = self.admin_site.name
        instance = self.get_object(request, unquote(object_id))

        if not self.has_change_permission(request, instance):
            raise PermissionDenied()

        if request.method != "POST":
            raise SuspiciousOperation()

        target_id = request.POST["target_id"]
        position = request.POST["position"]
        target_instance = self.get_object(request, target_id)

        self.do_move(instance, position, target_instance)

        return JsonResponse(dict(success=True))

    def do_move(self, instance, position, target_instance):
        if position == "before":
            instance.move_to(target_instance, "left")
        elif position == "after":
            instance.move_to(target_instance, "right")
        elif position == "inside":
            instance.move_to(target_instance)
        else:
            raise Exception("Unknown position")

        if self.trigger_save_after_move:
            instance.save()

    def get_change_list_for_tree(
        self: Union[ModelAdmin, "DjangoMpttAdminMixin"],
        request,
        node_id=None,
        max_level=None,
    ):
        request.current_app = self.admin_site.name

        return self.change_list_tree_class(
            request=request,
            model=self.model,
            model_admin=self,
            list_filter=self.get_list_filter(request),
            node_id=node_id,
            max_level=max_level,
        )

    def get_admin_url(self: ModelAdmin, name, args=None):
        opts = self.opts
        url_name = "admin:{0!s}_{1!s}_{2!s}".format(
            opts.app_label, util.get_model_name(self.model), name
        )
        return reverse(url_name, args=args, current_app=self.admin_site.name)

    def get_tree_data(
        self: Union[ModelAdmin, "DjangoMpttAdminMixin"], qs, max_level, filters_params
    ):
        pk_attname = self.opts.pk.attname

        preserved_filters = urlencode(
            {"_changelist_filters": urlencode(filters_params)}
        )

        def add_preserved_filters_to_url(url):
            return add_preserved_filters(
                {"preserved_filters": preserved_filters, "opts": self.opts}, url
            )

        def handle_create_node(instance, node_info):
            pk = getattr(instance, pk_attname)

            node_url = add_preserved_filters_to_url(
                self.get_admin_url("change", (quote(pk),))
            )

            node_info.update(
                url=node_url, move_url=self.get_admin_url("move", (quote(pk),))
            )

        return util.get_tree_from_queryset(
            qs, handle_create_node, max_level, self.item_label_field_name
        )

    def tree_json_view(self: Union[ModelAdmin, "DjangoMpttAdminMixin"], request):
        request.current_app = self.admin_site.name
        node_id = request.GET.get("node")

        def get_max_level():
            if node_id:
                node = self.model.objects.get(pk=node_id)
                return node.level + 1
            else:
                return self.tree_load_on_demand

        max_level = get_max_level()

        change_list = self.get_change_list_for_tree(request, node_id, max_level)

        qs = change_list.get_queryset(request)
        qs = self.filter_tree_queryset(qs, request)

        tree_data = self.get_tree_data(qs, max_level, change_list.get_filters_params())

        # Set safe to False because the data is a list instead of a dict
        return JsonResponse(tree_data, safe=False)

    def grid_view(
        self: Union[ModelAdmin, "DjangoMpttAdminMixin"], request, extra_context=None
    ):
        request.current_app = self.admin_site.name

        preserved_filters = self.get_preserved_filters(request)

        tree_url = add_preserved_filters(
            {"preserved_filters": preserved_filters, "opts": self.opts},
            self.get_admin_url("changelist"),
        )

        context = dict(tree_url=tree_url)

        if extra_context:
            context.update(extra_context)
        return super(DjangoMpttAdminMixin, self).changelist_view(request, context)

    def get_preserved_filters(self: Union[ModelAdmin, "DjangoMpttAdminMixin"], request):
        """
        Override `get_preserved_filters` to make sure that it returns the current filters for the grid view.
        """

        def must_return_current_filters():
            match = request.resolver_match

            if not self.preserve_filters or not match:
                return False
            else:
                opts = self.opts
                current_url = "{0!s}:{1!s}".format(match.app_name, match.url_name)
                grid_url = "admin:{0!s}_{1!s}_grid".format(
                    opts.app_label, opts.model_name
                )

                return current_url == grid_url

        if must_return_current_filters():
            # for the grid view return the current filters
            preserved_filters = request.GET.urlencode()
            return urlencode({"_changelist_filters": preserved_filters})
        else:
            return super(DjangoMpttAdminMixin, self).get_preserved_filters(request)

    def filter_tree_queryset(self, queryset, request):
        """
        Override 'filter_tree_queryset' to filter the queryset for the tree.
        """
        return queryset

    def get_changeform_initial_data(
        self: Union[ModelAdmin, "DjangoMpttAdminMixin"], request
    ):
        initial_data = super(DjangoMpttAdminMixin, self).get_changeform_initial_data(
            request=request
        )

        if "insert_at" in request.GET:
            initial_data[self.get_insert_at_field()] = request.GET.get("insert_at")

        return initial_data

    def get_insert_at_field(self):
        return "parent"

    def is_drag_and_drop_enabled(self) -> bool:
        # Override this method to disable drag-and-drop
        return True

    def get_tree_mouse_delay(self):
        return self.tree_mouse_delay
