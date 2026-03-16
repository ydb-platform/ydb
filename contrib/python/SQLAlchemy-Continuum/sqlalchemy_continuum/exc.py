class VersioningError(Exception):
    pass


class ClassNotVersioned(VersioningError):
    pass


class ImproperlyConfigured(VersioningError):
    pass
