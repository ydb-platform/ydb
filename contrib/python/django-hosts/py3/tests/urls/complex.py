from django.urls import re_path
from tests.views import test_view

urlpatterns = [
    re_path(r'^template/(?P<template>\w+)/$', test_view, name='complex-direct'),
]
