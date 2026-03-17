from django.conf.urls import url
from django.shortcuts import render

urlpatterns = [
    url(r'^multiple/$', render, name='multiple-direct'),
]
