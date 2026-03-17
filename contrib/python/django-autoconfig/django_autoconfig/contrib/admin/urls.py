import django
from django.conf.urls import include, url
try:
    from django.conf.urls import patterns
except ImportError:
    def patterns(_, *args):
        return args
from django.contrib import admin

admin.autodiscover()
# admin.site.urls should not be included since Django 1.9 (and breaks since Django 2.0).
# Additionally, urlpatterns shall be an array since Django 1.8.
if django.VERSION >= (1, 9):
    urlpatterns = [
        url('', admin.site.urls),
    ]
else:
    urlpatterns = patterns('',
        url('', include(admin.site.urls)),
    )
