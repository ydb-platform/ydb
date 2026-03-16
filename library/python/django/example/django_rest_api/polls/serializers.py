# coding: utf8
from __future__ import absolute_import, division, print_function, unicode_literals

from rest_framework import serializers

from library.python.django.example.django_rest_api.polls.models import Person, Poll, Choice


class PersonSerializer(serializers.ModelSerializer):
    class Meta:
        model = Person
        fields = ['name']


class PollSerializer(serializers.ModelSerializer):
    class Meta:
        model = Poll
        fields = '__all__'


class ChoiceSerializer(serializers.ModelSerializer):
    class Meta:
        model = Choice
        fields = '__all__'
