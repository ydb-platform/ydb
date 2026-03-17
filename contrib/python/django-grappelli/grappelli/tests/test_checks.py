# coding: utf-8

# DJANGO IMPORTS
from django.core.management import call_command
from django.test import TestCase

# GRAPPELLI IMPORTS
from grappelli.checks import check_model
from grappelli.tests.models import Entry


class ChecksTests(TestCase):

    def test_run_checks(self):
        # pytest-django doesn't run checks, but we should
        call_command('check')


class AutocompleteSearchFieldsChecksTests(TestCase):

    def test_passes_for_Entry(self):
        # Not strictly necessary as the check will run as part of the above
        assert check_model(Entry) == []

    def test_fails_for_Entry_broken_field_name(self):
        @staticmethod
        def broken():
            return ("tytle__icontains",)

        orig = Entry.__dict__['autocomplete_search_fields']
        try:
            Entry.autocomplete_search_fields = broken
            errors = check_model(Entry)
            assert len(errors) == 1
            assert (
                errors[0].msg ==
                'Model grappelli.entry returned bad entries for '
                'autocomplete_search_fields: tytle__icontains'
            )
        finally:
            Entry.autocomplete_search_fields = orig
