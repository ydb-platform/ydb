import warnings

from django.conf import settings

MODERATORS = getattr(settings, 'DJANGO_MODERATION_MODERATORS', ())
if MODERATORS:
    warnings.warn(
        "`%s` is deprecated, use `%s` instead." %
        ('DJANGO_MODERATION_MODERATORS', 'MODERATION_MODERATORS'))
MODERATORS = getattr(settings, 'MODERATION_MODERATORS', ())
