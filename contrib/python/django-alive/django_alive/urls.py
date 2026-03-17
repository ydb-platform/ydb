try:
    from django.urls import re_path
except ImportError:
    from django.conf.urls import url as re_path

from . import views

urlpatterns = [
    re_path(r"^alive/$", views.alive, name="alive_alive"),
    re_path(r"^health/$", views.healthcheck, name="alive_health"),
]
