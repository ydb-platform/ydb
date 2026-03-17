from .register import RegistrationError


def automoderate(instance, user):
    '''
    Auto moderates given model instance on user. Returns moderation status:
    0 - Rejected
    1 - Approved
    '''
    try:
        status = instance.moderated_object.automoderate(user)
    except AttributeError:
        msg = "%s has been registered with Moderation." % instance.__class__
        raise RegistrationError(msg)

    return status


def import_moderator(app):
    '''
    Import moderator module and register all models it contains with moderation
    '''
    from importlib import import_module

    # Yandex specific
    try:
        return import_module("%s.moderator" % app)
    except ImportError:
        pass
    # End Yandex specific

    try:
        app_path = import_module(app).__path__
    except AttributeError:
        return None

    try:
        import imp
        imp.find_module('moderator', app_path)
    except ImportError:
        return None

    module = import_module("%s.moderator" % app)

    return module


def auto_discover():
    '''
    Auto register all apps that have module moderator with moderation
    '''
    from django.conf import settings

    for app in [app for app in settings.INSTALLED_APPS if app != 'moderation']:
        import_moderator(app)
