from django.urls import path
from django.shortcuts import render

urlpatterns = [
    path('multiple/', render, name='multiple-direct'),
]
