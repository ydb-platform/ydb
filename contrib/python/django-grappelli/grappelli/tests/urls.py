# coding: utf-8

# DJANGO IMPORTS
from django.urls import re_path
from django.conf.urls import include

# GRAPPELLI IMPORTS
from grappelli.tests import admin


urlpatterns = [
    re_path(r'^admin/', admin.site.urls),
    re_path(r'^grappelli/', include('grappelli.urls'))
]
