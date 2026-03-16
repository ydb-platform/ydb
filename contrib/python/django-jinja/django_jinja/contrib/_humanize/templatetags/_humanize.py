from django.contrib.humanize.templatetags import humanize
from django_jinja import library


@library.filter
def ordinal(source):
    return humanize.ordinal(source)


@library.filter
def intcomma(source, use_l10n=True):
    return humanize.intcomma(source, use_l10n)


@library.filter
def intword(source):
    return humanize.intword(source)


@library.filter
def apnumber(source):
    return humanize.apnumber(source)


@library.filter
def naturalday(source, arg=None):
    return humanize.naturalday(source, arg)


@library.filter
def naturaltime(source):
    return humanize.naturaltime(source)
