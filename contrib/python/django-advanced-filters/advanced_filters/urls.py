from django import VERSION
if VERSION >= (2, 0):
    from django.urls import path
else:
    from django.conf.urls import url

from advanced_filters.views import GetFieldChoices

if VERSION >= (2, 0):
    urlpatterns = [
        path('field_choices/<model>/<field_name>/',
            GetFieldChoices.as_view(),
            name='afilters_get_field_choices'),

        # only to allow building dynamically
        path('field_choices/',
            GetFieldChoices.as_view(),
            name='afilters_get_field_choices'),
    ]
else:
    urlpatterns = [
        url(r'^field_choices/(?P<model>.+)/(?P<field_name>.+)/?',
            GetFieldChoices.as_view(),
            name='afilters_get_field_choices'),

        # only to allow building dynamically
        url(r'^field_choices/$',
            GetFieldChoices.as_view(),
            name='afilters_get_field_choices'),
    ]
