from django.urls import re_path
from django.contrib import admin
try:
    from django.urls import reverse
except ImportError:
    from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect
from .models import FileBrowser
from .settings import SHOW_IN_DASHBOARD


class FileBrowserAdmin(admin.ModelAdmin):
    actions = []

    def has_add_permission(self, request):
        return False

    def has_delete_permission(self, request, obj=None):
        return False

    def get_urls(self):
        opts = self.model._meta
        info = opts.app_label, (opts.model_name if hasattr(opts, 'model_name') else opts.module_name)
        return [
            re_path('^$', self.admin_site.admin_view(self.filebrowser_view), name='{0}_{1}_changelist'.format(*info)),
        ]

    def filebrowser_view(self, request):
        return HttpResponseRedirect(reverse('filebrowser:fb_browse'))


if SHOW_IN_DASHBOARD:
    admin.site.register(FileBrowser, FileBrowserAdmin)
