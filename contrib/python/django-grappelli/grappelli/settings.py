# coding: utf-8

# DJANGO IMPORTS
from django.conf import settings

# Admin Site Title
ADMIN_HEADLINE = getattr(settings, "GRAPPELLI_ADMIN_HEADLINE", 'Grappelli')
ADMIN_TITLE = getattr(settings, "GRAPPELLI_ADMIN_TITLE", 'Grappelli')

# Link to your Main Admin Site (no slashes at start and end)
# not needed anymore
ADMIN_URL = getattr(settings, "GRAPPELLI_ADMIN_URL", '/admin/')

# Autocomplete Limit
AUTOCOMPLETE_LIMIT = getattr(settings, "GRAPPELLI_AUTOCOMPLETE_LIMIT", 10)
# Alternative approach to define autocomplete search fields
AUTOCOMPLETE_SEARCH_FIELDS = getattr(settings, "GRAPPELLI_AUTOCOMPLETE_SEARCH_FIELDS", {})

# SWITCH_USER: Set True in order to activate this functionality
SWITCH_USER = getattr(settings, "GRAPPELLI_SWITCH_USER", False)
# SWITCH_USER_ORIGINAL: Defines if a user is allowed to login as another user.
# Gets a user object and returns True/False.
SWITCH_USER_ORIGINAL = getattr(settings, "GRAPPELLI_SWITCH_USER_ORIGINAL", lambda user: user.is_superuser)
# SWITCH_USER_ORIGINAL: Defines if a user is a valid target.
# Gets a user object and returns True/False.
SWITCH_USER_TARGET = getattr(settings, "GRAPPELLI_SWITCH_USER_TARGET", lambda original_user, user: user.is_staff and not user.is_superuser)
SWITCH_USER_REGEX = getattr(settings, "GRAPPELLI_SWITCH_USER_REGEX", r"\d+")

# CLEAN INPUT TYPES
# Replaces input types: search, email, url, tel, number, range, date
# month, week, time, datetime, datetime-local, color
# due to browser inconsistencies.
# see see https://code.djangoproject.com/ticket/23075
CLEAN_INPUT_TYPES = getattr(settings, "GRAPPELLI_CLEAN_INPUT_TYPES", True)
