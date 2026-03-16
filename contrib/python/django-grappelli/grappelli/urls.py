# coding: utf-8

from django.urls import re_path

from .views.related import AutocompleteLookup, M2MLookup, RelatedLookup
from .views.switch import switch_user
from .settings import SWITCH_USER_REGEX


urlpatterns = [

    # FOREIGNKEY & GENERIC LOOKUP
    re_path(r'^lookup/related/$', RelatedLookup.as_view(), name="grp_related_lookup"),
    re_path(r'^lookup/m2m/$', M2MLookup.as_view(), name="grp_m2m_lookup"),
    re_path(r'^lookup/autocomplete/$', AutocompleteLookup.as_view(), name="grp_autocomplete_lookup"),

    # SWITCH USER
    re_path(r'^switch/user/(?P<object_id>{})/$'.format(SWITCH_USER_REGEX), switch_user, name="grp_switch_user"),
]
