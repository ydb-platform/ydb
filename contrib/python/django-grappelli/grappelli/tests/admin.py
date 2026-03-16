# coding: utf-8

# DJANGO IMPORTS
from django.contrib import admin

# PROJECT IMPORTS
from grappelli.tests.models import Category, Entry

site = admin.AdminSite(name="Admin Site")


class CategoryOptions(admin.ModelAdmin):
    list_display = ("id", "name",)
    list_display_links = ("name",)


class EntryOptions(admin.ModelAdmin):
    list_display = ("id", "title", "category", "category_alt", "user",)
    list_display_links = ("title",)

    def get_queryset(self, request):
        qs = super(EntryOptions, self).get_queryset(request)
        if request.user.is_superuser:
            return qs
        return qs.filter(user=request.user)


site.register(Category, CategoryOptions)
site.register(Entry, EntryOptions)
