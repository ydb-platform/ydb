import time

from django.core.cache import cache


_other_allowed_chars = frozenset(['_', '.', '-'])


class BaseThrottle(object):
    """
    A simplified, swappable base class for throttling.

    Does nothing save for simulating the throttling API and implementing
    some common bits for the subclasses.

    Accepts a number of optional kwargs::

        * ``throttle_at`` - the number of requests at which the user should
          be throttled. Default is 150 requests.
        * ``timeframe`` - the length of time (in seconds) in which the user
          make up to the ``throttle_at`` requests. Default is 3600 seconds (
          1 hour).
        * ``expiration`` - the length of time to retain the times the user
          has accessed the api in the cache. Default is 604800 (1 week).
    """
    def __init__(self, throttle_at=150, timeframe=3600, expiration=None):
        self.throttle_at = throttle_at
        # In seconds, please.
        self.timeframe = timeframe

        if expiration is None:
            # Expire in a week.
            expiration = 604800

        self.expiration = int(expiration)

    def convert_identifier_to_key(self, identifier):
        """
        Takes an identifier (like a username or IP address) and converts it
        into a key usable by the cache system.
        """
        bits = [
            char
            for char in identifier
            if char.isalnum() or char in _other_allowed_chars
        ]
        bits.append('_accesses')

        return ''.join(bits)

    def should_be_throttled(self, identifier, **kwargs):
        """
        Returns whether or not the user has exceeded their throttle limit. If
        throttled, can return either True, and int specifying the number of
        seconds to wait, or a datetime object specifying when to retry the
        request.

        Always returns ``False``, as this implementation does not actually
        throttle the user.
        """
        return False

    def accessed(self, identifier, **kwargs):
        """
        Handles recording the user's access.

        Does nothing in this implementation.
        """
        pass


class CacheThrottle(BaseThrottle):
    """
    A throttling mechanism that uses just the cache.
    """
    def should_be_throttled(self, identifier, **kwargs):
        """
        Returns whether or not the user has exceeded their throttle limit. If
        throttled, can return either True, and int specifying the number of
        seconds to wait, or a datetime object specifying when to retry the
        request.

        Maintains a list of timestamps when the user accessed the api within
        the cache.

        Returns ``False`` if the user should NOT be throttled or ``True`` if
        the user should be throttled.
        """
        key = self.convert_identifier_to_key(identifier)

        # Weed out anything older than the timeframe.
        now = int(time.time())
        timeframe = int(self.timeframe)
        throttle_at = int(self.throttle_at)

        minimum_time = now - timeframe
        times_accessed = [access for access in cache.get(key, []) if access >= minimum_time]
        cache.set(key, times_accessed, self.expiration)

        if len(times_accessed) >= throttle_at:
            # Throttle them.
            return timeframe - (now - times_accessed[-throttle_at])

        # Let them through.
        return False

    def accessed(self, identifier, **kwargs):
        """
        Handles recording the user's access.

        Stores the current timestamp in the "accesses" list within the cache.
        """
        key = self.convert_identifier_to_key(identifier)
        times_accessed = cache.get(key, [])
        times_accessed.append(int(time.time()))
        cache.set(key, times_accessed, self.expiration)


class CacheDBThrottle(CacheThrottle):
    """
    A throttling mechanism that uses the cache for actual throttling but
    writes-through to the database.

    This is useful for tracking/aggregating usage through time, to possibly
    build a statistics interface or a billing mechanism.
    """
    def accessed(self, identifier, **kwargs):
        """
        Handles recording the user's access.

        Does everything the ``CacheThrottle`` class does, plus logs the
        access within the database using the ``ApiAccess`` model.
        """
        # Do the import here, instead of top-level, so that the model is
        # only required when using this throttling mechanism.
        from tastypie.models import ApiAccess
        super(CacheDBThrottle, self).accessed(identifier, **kwargs)
        # Write out the access to the DB for logging purposes.
        ApiAccess.objects.create(
            identifier=identifier,
            url=kwargs.get('url', ''),
            request_method=kwargs.get('request_method', '')
        )
