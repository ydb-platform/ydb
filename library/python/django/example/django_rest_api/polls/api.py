# coding: utf8
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import viewsets

from library.python.django.example.django_rest_api.polls.models import Choice, Person, Poll
from library.python.django.example.django_rest_api.polls.serializers import (
    ChoiceSerializer, PersonSerializer, PollSerializer
)


class ChoiceViewSet(viewsets.ModelViewSet):
    queryset = Choice.objects.all()
    serializer_class = ChoiceSerializer


class PersonViewSet(viewsets.ModelViewSet):
    queryset = Person.objects.all()
    serializer_class = PersonSerializer


class PollViewSet(viewsets.ModelViewSet):
    queryset = Poll.objects.all()
    serializer_class = PollSerializer
