# coding: utf-8


class Registry(object):
    """
    Registry for application dashboards.
    """
    registry = {}

    def register(cls, klass, app_name):
        from grappelli.dashboard.dashboards import Dashboard
        if not issubclass(klass, Dashboard):
            raise ValueError('%s is not an instance of Dashboard' % klass)
        if app_name in cls.registry:
            raise ValueError('A dashboard has already been registered for '
                             'the application "%s"', app_name)
        cls.registry[app_name] = klass
    register = classmethod(register)


def register(cls, *args, **kwargs):
    """
    Register a custom dashboard into the global registry.
    """
    Registry.register(cls, *args, **kwargs)


def autodiscover(blacklist=[]):
    """
    Automagically discover custom dashboards and menus for installed apps.
    Optionally you can pass a ``blacklist`` of apps that you don't want to
    provide their own app index dashboard.
    """
    import imp
    from importlib import import_module
    from django.conf import settings

    blacklist.append('grappelli')
    blacklist.append('grappelli.dashboard')

    for app in settings.INSTALLED_APPS:
        # skip blacklisted apps
        if app in blacklist:
            continue

        # try to import the app
        try:
            app_path = import_module(app).__path__
        except AttributeError:
            continue

        # try to find a app.dashboard module
        try:
            imp.find_module('dashboard', app_path)
        except ImportError:
            continue

        # looks like we found it so import it !
        import_module('%s.dashboard' % app)
