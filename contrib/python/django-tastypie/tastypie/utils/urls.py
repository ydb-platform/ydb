from django.conf import settings


_trailing_slash = '/?' if getattr(settings, 'TASTYPIE_ALLOW_MISSING_SLASH', False) else '/'


# for backwards compatibility where 3rd parties still call this like a function.
class CallableUnicode(str):
    def __call__(self):
        return self


trailing_slash = CallableUnicode(_trailing_slash)
