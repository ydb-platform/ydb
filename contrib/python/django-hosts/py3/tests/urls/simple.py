from django.urls import path
from django.shortcuts import render

urlpatterns = [
    path('simple/', render, name='simple-direct'),
]
