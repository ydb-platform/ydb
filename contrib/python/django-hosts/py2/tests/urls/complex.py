from django.conf.urls import url
from tests.views import test_view

urlpatterns = [
    url(r'^template/(?P<template>\w+)/$', test_view, name='complex-direct'),
]
