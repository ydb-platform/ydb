# coding: utf8
from django.db import models


class Poll(models.Model):
    question = models.CharField(max_length=200)

    def __str__(self):
        return self.question


class Choice(models.Model):
    poll = models.ForeignKey(Poll, on_delete=models.CASCADE)
    choice_text = models.CharField(max_length=200)
    votes = models.IntegerField(default=0)

    def __str__(self):
        return self.choice_text


class Person(models.Model):
    name = models.CharField(max_length=100)
    passport = models.CharField(max_length=20, null=True)

    def __str__(self):
        return self.name
