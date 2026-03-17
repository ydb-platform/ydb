from django.urls import path

from waffle.views import wafflejs, waffle_json

urlpatterns = [
    path('wafflejs', wafflejs, name='wafflejs'),
    path('waffle_status', waffle_json, name='waffle_status'),
]
