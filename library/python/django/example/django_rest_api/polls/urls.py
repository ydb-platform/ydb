# coding: utf8

from django.conf import settings
from django.conf.urls import url, static
from django.urls import path, include
from rest_framework import routers
from rest_framework.schemas import get_schema_view

from library.python.django.example.django_rest_api.polls import views
from library.python.django.example.django_rest_api.polls import api

app_name = "library.python.django.example.django_rest_api.polls"

router = routers.DefaultRouter()
router.register(r'choice', api.ChoiceViewSet)
router.register(r'person', api.PersonViewSet)
router.register(r'poll', api.PollViewSet)


urlpatterns = [
    path('', include(router.urls)),
    url(r'^$', views.IndexView.as_view(), name='index'),
    url(r'^(?P<pk>\d+)/$', views.DetailView.as_view(), name='detail'),
    url(r'^(?P<pk>\d+)/results/$', views.ResultsView.as_view(), name='results'),
    url(r'^(?P<poll_id>\d+)/vote/$', views.vote, name='vote'),

    path('openapi', get_schema_view(
        title="Your Project",
        description="API for all things …",
    ), name='openapi-schema'),
] + static.static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)
