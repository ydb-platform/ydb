from hashlib import sha1
import hmac
import time

from django.conf import settings
from django.db import models

from tastypie.utils import now


class ApiAccess(models.Model):
    """A simple model for use with the ``CacheDBThrottle`` behaviors."""
    identifier = models.CharField(max_length=255)
    url = models.TextField(blank=True, default='')
    request_method = models.CharField(max_length=10, blank=True, default='')
    accessed = models.PositiveIntegerField()

    def __str__(self):
        return "%s @ %s" % (self.identifier, self.accessed)

    def save(self, *args, **kwargs):
        self.accessed = int(time.time())
        return super(ApiAccess, self).save(*args, **kwargs)


if 'django.contrib.auth' in settings.INSTALLED_APPS:
    import uuid
    from tastypie.compat import AUTH_USER_MODEL

    class ApiKey(models.Model):
        user = models.OneToOneField(AUTH_USER_MODEL, related_name='api_key', on_delete=models.CASCADE)
        key = models.CharField(max_length=128, blank=True, default='', db_index=True)
        created = models.DateTimeField(default=now)

        def __str__(self):
            return u"%s for %s" % (self.key, self.user)

        def save(self, *args, **kwargs):
            if not self.key:
                self.key = self.generate_key()

            return super(ApiKey, self).save(*args, **kwargs)

        def generate_key(self):
            # Get a random UUID.
            new_uuid = uuid.uuid4()
            # Hmac that beast.
            return hmac.new(new_uuid.bytes, digestmod=sha1).hexdigest()

        class Meta:
            abstract = getattr(settings, 'TASTYPIE_ABSTRACT_APIKEY', False)

    def create_api_key(sender, instance, created, **kwargs):
        """
        A signal for hooking up automatic ``ApiKey`` creation.
        """
        if kwargs.get('raw', False) is False and created is True:
            ApiKey.objects.create(user=instance)
