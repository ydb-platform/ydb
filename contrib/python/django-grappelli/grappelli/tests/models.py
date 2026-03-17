# coding: utf-8

from django.conf import settings
from django.contrib.auth.models import User
from django.db import models


class Category(models.Model):
    name = models.CharField("Title", max_length=50, unique=True)

    class Meta:
        app_label = "grappelli"
        verbose_name = "Category"
        verbose_name_plural = "Categories"

    def __str__(self):
        return self.name

    @staticmethod
    def autocomplete_search_fields():
        return ("id__iexact", "name__icontains",)

    def related_label(self):
        return "%s (%s)" % (self.name, self.id)


class Entry(models.Model):
    title = models.CharField("Title", max_length=200)
    category = models.ForeignKey(Category, on_delete=models.SET_NULL, related_name="entries", blank=True, null=True)
    category_alt = models.ForeignKey(Category, on_delete=models.SET_NULL, related_name="entriesalt", to_field="name", blank=True, null=True)
    date = models.DateTimeField("Date")
    body = models.TextField("Body", blank=True)
    user = models.ForeignKey(getattr(settings, 'AUTH_USER_MODEL', User), on_delete=models.CASCADE, related_name="entries")
    createdate = models.DateField("Date (Create)", auto_now_add=True)
    updatedate = models.DateField("Date (Update)", auto_now=True)

    class Meta:
        app_label = "grappelli"
        verbose_name = "Entry"
        verbose_name_plural = "Entries"
        ordering = ["-date", "title"]

    def __str__(self):
        return self.title

    @staticmethod
    def autocomplete_search_fields():
        return ("id__iexact", "title__icontains",)
