try:
    from django.urls import path
    old_django = False
except ImportError:
    from django.conf.urls import url
    old_django = True

from ajax_select import views

if old_django:
    urlpatterns = [url(r"^ajax_lookup/(?P<channel>[-\w]+)$", views.ajax_lookup, name="ajax_lookup")]
else:
    urlpatterns = [path("ajax_lookup/<channel>", views.ajax_lookup, name="ajax_lookup")]
