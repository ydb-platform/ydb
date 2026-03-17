from typing import Tuple, Type, Optional

from django import forms
from django.conf import settings as django_settings
from django.urls import re_path, path
from django.contrib import admin
from django.contrib import messages
from django.contrib.admin.sites import NotRegistered
from django.http import HttpResponseRedirect, HttpRequest, HttpResponse
from django.urls import get_urlconf, get_resolver
from django.utils.translation import gettext_lazy as _

from .fields import TreeItemChoiceField
from .settings import MODEL_TREE, MODEL_TREE_ITEM
from .utils import get_tree_model, get_tree_item_model, get_app_n_model

if False:  # pragma: nocover
    from .models import TreeItemBase, TreeBase  # noqa


SMUGGLER_INSTALLED = 'smuggler' in django_settings.INSTALLED_APPS

MODEL_TREE_CLASS = get_tree_model()
MODEL_TREE_ITEM_CLASS = get_tree_item_model()

_TREE_ADMIN = lambda: TreeAdmin
_ITEM_ADMIN = lambda: TreeItemAdmin


def get_model_url_name(model_nfo: Tuple[str, str], page: str, with_namespace: bool = False) -> str:
    """Returns a URL for a given Tree admin page type."""
    prefix = ''
    if with_namespace:
        prefix = 'admin:'
    return (f'{prefix}%s_{page}' % '%s_%s' % model_nfo).lower()


def get_tree_url_name(page: str, with_namespace: bool = False) -> str:
    """Returns a URL for a given Tree admin page type."""
    return get_model_url_name(get_app_n_model('MODEL_TREE'), page, with_namespace)


def get_tree_item_url_name(page: str, with_namespace: bool = False) -> str:
    """Returns a URL for a given Tree Item admin page type."""
    return get_model_url_name(get_app_n_model('MODEL_TREE_ITEM'), page, with_namespace)


_TREE_URLS = {
    'app': get_app_n_model('MODEL_TREE')[0],
    'change': get_tree_url_name('change', True),
    'changelist': get_tree_url_name('changelist', True),
    'treeitem_change': get_tree_item_url_name('change', True)
}


def _reregister_tree_admin():
    """Forces unregistration of tree admin class with following re-registration."""
    try:
        admin.site.unregister(MODEL_TREE_CLASS)

    except NotRegistered:
        pass

    admin.site.register(MODEL_TREE_CLASS, _TREE_ADMIN())


def override_tree_admin(admin_class: Type['TreeAdmin']):
    """Sets a class that should be used instead of TreeAdmin
    to represent trees in the Admin interface.
    Note that the class must inherit from TreeAdmin.

    """
    global _TREE_ADMIN
    _TREE_ADMIN = lambda: admin_class
    _reregister_tree_admin()


def override_item_admin(admin_class: Type['TreeItemAdmin']):
    """Sets a class that should be used instead of TreeItemAdmin
    to represent tree items in the Admin interface.
    Note that the class must inherit from TreeItemAdmin.

    """
    global _ITEM_ADMIN
    _ITEM_ADMIN = lambda: admin_class
    _reregister_tree_admin()


class TreeItemForm(forms.ModelForm):
    """Item form with swapped parent field."""

    parent = TreeItemChoiceField()


class TreeItemAdmin(admin.ModelAdmin):

    form = TreeItemForm

    exclude = ('tree', 'sort_order')

    fieldsets = (
        (_('Basic settings'), {
            'fields': ('parent', 'title', 'url',)
        }),
        (_('Access settings'), {
            'classes': ('collapse',),
            'fields': ('access_loggedin', 'access_guest', 'access_restricted', 'access_permissions', 'access_perm_type')
        }),
        (_('Display settings'), {
            'classes': ('collapse',),
            'fields': ('hidden', 'inmenu', 'inbreadcrumbs', 'insitetree')
        }),
        (_('Additional settings'), {
            'classes': ('collapse',),
            'fields': ('hint', 'description', 'alias', 'urlaspattern')
        }),
    )

    filter_horizontal = ('access_permissions',)
    change_form_template = 'admin/sitetree/treeitem/change_form.html'
    delete_confirmation_template = 'admin/sitetree/treeitem/delete_confirmation.html'

    def formfield_for_manytomany(self, db_field, request=None, **kwargs):

        # The same as for GroupAdmin
        # Avoid a major performance hit resolving permission names which
        # triggers a content_type load:
        if db_field.name == 'access_permissions':
            objects = db_field.remote_field.model.objects
            qs = kwargs.get('queryset', objects)
            kwargs['queryset'] = qs.select_related('content_type')

        return super().formfield_for_manytomany(db_field, request=request, **kwargs)

    def _redirect(self, request: HttpRequest, response: HttpResponse) -> HttpResponse:
        """Generic redirect for item editor."""

        if '_addanother' in request.POST:
            return HttpResponseRedirect('../item_add/')

        elif '_save' in request.POST:
            return HttpResponseRedirect('../')

        elif '_continue' in request.POST:
            return response

        return HttpResponseRedirect('')

    def response_add(self, request, obj, post_url_continue=None, **kwargs):
        """Redirects to the appropriate items' 'continue' page on item add.

        As we administer tree items within tree itself, we
        should make some changes to redirection process.

        """
        if post_url_continue is None:
            post_url_continue = f'../item_{obj.pk}/'

        return self._redirect(request, super().response_add(request, obj, post_url_continue))

    def response_change(self, request, obj, **kwargs):
        """Redirects to the appropriate items' 'add' page on item change.

        As we administer tree items within tree itself, we
        should make some changes to redirection process.

        """
        return self._redirect(request, super().response_change(request, obj))

    def get_form(self, request, obj=None, **kwargs):
        """Returns modified form for TreeItem model.
        'Parent' field choices are built by sitetree itself.

        """
        if obj is not None and obj.parent is not None:
            self.previous_parent = obj.parent

        form = super().get_form(request, obj, **kwargs)
        form.base_fields['parent'].choices_init(self.tree)

        # Try to resolve all currently registered url names including those in namespaces.
        if not getattr(self, 'known_url_names', False):
            self.known_url_names = []
            self.known_url_rules = []

            resolver = get_resolver(get_urlconf())

            for ns, (url_prefix, ns_resolver) in resolver.namespace_dict.items():
                if ns != 'admin':
                    self._stack_known_urls(ns_resolver.reverse_dict, ns)

            self._stack_known_urls(resolver.reverse_dict)
            self.known_url_rules = sorted(self.known_url_rules)

        form.known_url_names_hint = _(
            'You are seeing this warning because "URL as Pattern" option is active and pattern entered above '
            'seems to be invalid. Currently registered URL pattern names and parameters: ')

        form.known_url_names = self.known_url_names
        form.known_url_rules = self.known_url_rules

        return form

    def _stack_known_urls(self, reverse_dict, ns=None):
        for url_name, url_rules in reverse_dict.items():
            if isinstance(url_name, str):
                if ns is not None:
                    url_name = f'{ns}:{url_name}'
                self.known_url_names.append(url_name)
                self.known_url_rules.append(f"<b>{url_name}</b> {' '.join(url_rules[0][0][1])}")

    def get_tree(self, request: HttpRequest, tree_id: Optional[int], item_id: Optional[int] = None) -> 'TreeBase':
        """Fetches Tree for current or given TreeItem."""
        if tree_id is None:
            tree_id = self.get_object(request, item_id).tree_id

        self.tree = MODEL_TREE_CLASS._default_manager.get(pk=tree_id)
        self.tree.verbose_name_plural = self.tree._meta.verbose_name_plural
        self.tree.urls = _TREE_URLS

        return self.tree

    def item_add(self, request: HttpRequest, tree_id: int) -> HttpResponse:
        tree = self.get_tree(request, tree_id)
        return self.add_view(request, extra_context={'tree': tree})

    def item_edit(self, request: HttpRequest, item_id: int, tree_id: int = None) -> HttpResponse:
        tree = self.get_tree(request, tree_id, item_id)
        return self.change_view(request, item_id, extra_context={'tree': tree})

    def item_delete(self, request: HttpRequest, item_id: int, tree_id: int = None) -> HttpResponse:
        tree = self.get_tree(request, tree_id, item_id)
        return self.delete_view(request, item_id, extra_context={'tree': tree})

    def item_history(self, request: HttpRequest, item_id: int, tree_id: int = None) -> HttpResponse:
        tree = self.get_tree(request, tree_id, item_id)
        return self.history_view(request, item_id, extra_context={'tree': tree})

    def item_move(self, request: HttpRequest, tree_id: int, item_id: int, direction: str) -> HttpResponse:
        """Moves item up or down by swapping 'sort_order' field values of neighboring items."""
        current_item = MODEL_TREE_ITEM_CLASS._default_manager.get(pk=item_id)

        sort_order = 'sort_order' if direction == 'up' else '-sort_order'

        siblings = MODEL_TREE_ITEM_CLASS._default_manager.filter(
            parent=current_item.parent,
            tree=current_item.tree
        ).order_by(sort_order)

        previous_item = None
        for item in siblings:
            if item != current_item:
                previous_item = item
            else:
                break

        if previous_item is not None:
            current_item_sort_order = current_item.sort_order
            previous_item_sort_order = previous_item.sort_order

            current_item.sort_order = previous_item_sort_order
            previous_item.sort_order = current_item_sort_order

            current_item.save()
            previous_item.save()

        return HttpResponseRedirect('../../')

    def save_model(self, request: HttpRequest, obj: 'TreeItemBase', form, change: bool):
        """Saves TreeItem model under certain Tree.
        Handles item's parent assignment exception.

        """
        if change:
            # No, you're not allowed to make item parent of itself
            if obj.parent is not None and obj.parent.id == obj.id:
                obj.parent = self.previous_parent
                messages.warning(
                    request, _("Item's parent left unchanged. Item couldn't be parent to itself."), '', True)

        obj.tree = self.tree
        obj.save()


def redirects_handler(*args, **kwargs):
    """Fixes Admin contrib redirects compatibility problems
    introduced in Django 1.4 by url handling changes.

    """
    path = args[0].path
    shift = '../'

    if 'delete' in path:
        # Weird enough 'delete' is not handled by TreeItemAdmin::response_change().
        shift += '../'

    elif 'history' in path:
        if 'item_id' not in kwargs:
            # Encountered request from history page to return to tree layout page.
            shift += '../'

    return HttpResponseRedirect(path + shift)


class TreeAdmin(admin.ModelAdmin):

    list_display = ('alias', 'title')
    list_display_links = ('title', 'alias')
    search_fields = ['title', 'alias']
    ordering = ['title', 'alias']
    actions = None
    change_form_template = 'admin/sitetree/tree/change_form.html'

    def __init__(self, *args, **kwargs):

        if SMUGGLER_INSTALLED:
            self.change_list_template = 'admin/sitetree/tree/change_list_.html'

        super().__init__(*args, **kwargs)
        self.tree_admin = _ITEM_ADMIN()(MODEL_TREE_ITEM_CLASS, admin.site)

    def render_change_form(self, request, context, **kwargs):
        context['icon_ext'] = '.svg'
        return super().render_change_form(request, context, **kwargs)

    def get_urls(self):
        """Manages not only TreeAdmin URLs but also TreeItemAdmin URLs."""
        urls = super().get_urls()

        prefix_change = 'change/'

        sitetree_urls = [
            path('change/', redirects_handler, name=get_tree_item_url_name('changelist')),

            re_path(fr'^((?P<tree_id>\d+)/)?{prefix_change}item_add/$',
                self.admin_site.admin_view(self.tree_admin.item_add), name=get_tree_item_url_name('add')),

            re_path(fr'^(?P<tree_id>\d+)/{prefix_change}item_(?P<item_id>\d+)/$',
                self.admin_site.admin_view(self.tree_admin.item_edit), name=get_tree_item_url_name('change')),

            re_path(fr'^{prefix_change}item_(?P<item_id>\d+)/$',
                self.admin_site.admin_view(self.tree_admin.item_edit), name=get_tree_item_url_name('change')),

            re_path(fr'^((?P<tree_id>\d+)/)?{prefix_change}item_(?P<item_id>\d+)/delete/$',
                self.admin_site.admin_view(self.tree_admin.item_delete), name=get_tree_item_url_name('delete')),

            re_path(fr'^((?P<tree_id>\d+)/)?{prefix_change}item_(?P<item_id>\d+)/history/$',
                self.admin_site.admin_view(self.tree_admin.item_history), name=get_tree_item_url_name('history')),

            re_path(fr'^(?P<tree_id>\d+)/{prefix_change}item_(?P<item_id>\d+)/move_(?P<direction>(up|down))/$',
                self.admin_site.admin_view(self.tree_admin.item_move), name=get_tree_item_url_name('move')),
        ]
        if SMUGGLER_INSTALLED:
            sitetree_urls += (
                path('dump_all/', self.admin_site.admin_view(self.dump_view), name='sitetree_dump'),
            )

        return sitetree_urls + urls

    @classmethod
    def dump_view(cls, request: HttpRequest) -> HttpResponse:
        """Dumps sitetrees with items using django-smuggler.

        :param request:

        """
        from smuggler.views import dump_to_response
        return dump_to_response(request, [MODEL_TREE, MODEL_TREE_ITEM], filename_prefix='sitetrees')


_reregister_tree_admin()
