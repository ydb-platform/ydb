from django import VERSION as DJANGO_VERSION


if DJANGO_VERSION[0] < 3:
    from django.utils.encoding import force_text
else:
    from django.utils.encoding import force_str as force_text
