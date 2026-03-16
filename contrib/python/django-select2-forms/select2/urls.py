from django.urls import re_path

import select2.views


urlpatterns = [
    re_path(r'^fetch_items/(?P<app_label>[^\/]+)/(?P<model_name>[^\/]+)/(?P<field_name>[^\/]+)/$',
        select2.views.fetch_items, name='select2_fetch_items'),
    re_path(r'^init_selection/(?P<app_label>[^\/]+)/(?P<model_name>[^\/]+)/(?P<field_name>[^\/]+)/$',
        select2.views.init_selection, name='select2_init_selection'),
]
