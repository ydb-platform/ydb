# coding: utf-8

"""
Admin ui common utilities.
"""

# PYTHON IMPORTS
from __future__ import unicode_literals
from fnmatch import fnmatch
from importlib import import_module

# DJANGO IMPORTS
from django.conf import settings
from django.contrib import admin
from django.urls import reverse


def _get_dashboard_cls(dashboard_cls, context):
    if isinstance(dashboard_cls, dict):
        curr_url = context.get('request').path
        for key in dashboard_cls:
            admin_site_mod, admin_site_inst = key.rsplit('.', 1)
            admin_site_mod = import_module(admin_site_mod)
            admin_site = getattr(admin_site_mod, admin_site_inst)
            admin_url = reverse('%s:index' % admin_site.name)
            if curr_url.startswith(admin_url):
                mod, inst = dashboard_cls[key].rsplit('.', 1)
                mod = import_module(mod)
                return getattr(mod, inst)
    else:
        mod, inst = dashboard_cls.rsplit('.', 1)
        mod = import_module(mod)
        return getattr(mod, inst)
    raise ValueError('Dashboard matching "%s" not found' % dashboard_cls)


def get_index_dashboard(context):
    """
    Returns the admin dashboard defined in settings (or the default one).
    """

    return _get_dashboard_cls(getattr(
        settings,
        'GRAPPELLI_INDEX_DASHBOARD',
        'grappelli.dashboard.dashboards.DefaultIndexDashboard'
    ), context)()


def get_admin_site(context=None, request=None):
    dashboard_cls = getattr(
        settings,
        'GRAPPELLI_INDEX_DASHBOARD',
        'grappelli.dashboard.dashboards.DefaultIndexDashboard'
    )

    if isinstance(dashboard_cls, dict):
        if context:
            request = context.get('request')
        curr_url = request.path
        for key in dashboard_cls:
            mod, inst = key.rsplit('.', 1)
            mod = import_module(mod)
            admin_site = getattr(mod, inst)
            admin_url = reverse('%s:index' % admin_site.name)
            if curr_url.startswith(admin_url):
                return admin_site
    else:
        return admin.site
    raise ValueError('Admin site matching "%s" not found' % dashboard_cls)


def get_admin_site_name(context):
    return get_admin_site(context).name


def get_avail_models(request):
    """ Returns (model, perm,) for all models user can possibly see """
    items = []
    admin_site = get_admin_site(request=request)

    for model, model_admin in admin_site._registry.items():
        perms = model_admin.get_model_perms(request)
        if True not in perms.values():
            continue
        items.append((model, perms,))
    return items


def filter_models(request, models, exclude):
    """
    Returns (model, perm,) for all models that match models/exclude patterns
    and are visible by current user.
    """
    items = get_avail_models(request)
    included = []
    full_name = lambda model: '%s.%s' % (model.__module__, model.__name__)

    # I believe that that implemented
    # O(len(patterns)*len(matched_patterns)*len(all_models))
    # algorythm is fine for model lists because they are small and admin
    # performance is not a bottleneck. If it is not the case then the code
    # should be optimized.

    if len(models) == 0:
        included = items
    else:
        for pattern in models:
            pattern_items = []
            for item in items:
                model, perms = item
                if fnmatch(full_name(model), pattern) and item not in included:
                    pattern_items.append(item)
            pattern_items.sort(key=lambda x: str(x[0]._meta.verbose_name_plural.encode('utf-8')))
            included.extend(pattern_items)

    result = included[:]
    for pattern in exclude:
        for item in included:
            model, perms = item
            if fnmatch(full_name(model), pattern):
                try:
                    result.remove(item)
                except ValueError:  # if the item was already removed skip
                    pass
    return result


class AppListElementMixin(object):
    """
    Mixin class used by both the AppListDashboardModule and the
    AppListMenuItem (to honor the DRY concept).
    """

    def _visible_models(self, request):
        included = self.models[:]
        excluded = self.exclude[:]
        if not self.models and not self.exclude:
            included = ["*"]
        return filter_models(request, included, excluded)

    def _get_admin_app_list_url(self, model, context):
        """
        Returns the admin change url.
        """
        app_label = model._meta.app_label
        return reverse('%s:app_list' % get_admin_site_name(context),
                       args=(app_label,))

    def _get_admin_change_url(self, model, context):
        """
        Returns the admin change url.
        """
        app_label = model._meta.app_label
        return reverse('%s:%s_%s_changelist' % (get_admin_site_name(context),
                                                app_label,
                                                model.__name__.lower()))

    def _get_admin_add_url(self, model, context):
        """
        Returns the admin add url.
        """
        app_label = model._meta.app_label
        return reverse('%s:%s_%s_add' % (get_admin_site_name(context),
                                         app_label,
                                         model.__name__.lower()))
