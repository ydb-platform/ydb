try:
    from django.urls import re_path

except ImportError:
    # fallback for Django < 4.0
    from django.conf.urls import re_path

from django.contrib import admin, messages
from django.template.response import TemplateResponse
from django.utils.translation import gettext_lazy as _


class OnePageAdmin(admin.ModelAdmin):

    def get_urls(self):
        meta = self.model._meta

        urlpatterns = [
            re_path(
                '^$',
                self.admin_site.admin_view(self.view_onepage),
                name=f'{meta.app_label}_{meta.model_name}_changelist'
            )
        ]

        return urlpatterns

    def view_onepage(self, request):

        # Not on the top to protect from module caching
        # caused by 1. import from manage command 2. from uWSGI
        from uwsgiconf import uwsgi

        is_stub = uwsgi.is_stub

        if is_stub:
            self.message_user(
                request,
                _("This site is not served by uWSGI. Certain functionality or data may be unavailable."),
                level=messages.WARNING,
            )

        context = dict(
            self.admin_site.each_context(request),

            stub=is_stub,
            title=self.opts.verbose_name,
            model_meta=self.model._meta,
        )
        self.contribute_onepage_context(request, context)
        return TemplateResponse(request, 'admin/uwsgify/onepage.html', context)

    def contribute_onepage_context(self, request, context):  # pragma: nocover
        raise NotImplementedError
