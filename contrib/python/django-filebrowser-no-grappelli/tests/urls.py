from django.conf.urls import include
from django.urls import re_path
from django.contrib import admin
from django import VERSION as DJANGO_VERSION

from filebrowser.sites import site

admin.autodiscover()

if DJANGO_VERSION >= (1, 9):
    urlpatterns = [
        re_path(r'^admin/filebrowser/', include(site.urls[:2], namespace=site.urls[2])),
        re_path(r'^admin/', include(admin.site.urls[:2], namespace=admin.site.urls[2])),
    ]
else:
    urlpatterns = [
        re_path(r'^admin/filebrowser/', include(site.urls)),
        re_path(r'^admin/', include(admin.site.urls)),
    ]
