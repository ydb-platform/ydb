# -*- coding: utf-8 -*-


def autodiscover():
    """
    Auto-discover INSTALLED_APPS translation.py modules and fail silently when
    not present. This forces an import on them to register.
    Also import explicit modules.
    """
    import os
    import sys
    import copy
    from django.utils.module_loading import module_has_submodule
    from modeltranslation.translator import translator
    from modeltranslation.settings import TRANSLATION_FILES, DEBUG

    from importlib import import_module
    from django.apps import apps

    mods = [(app_config.name, app_config.module) for app_config in apps.get_app_configs()]

    for (app, mod) in mods:
        # Attempt to import the app's translation module.
        module = '%s.translation' % app
        before_import_registry = copy.copy(translator._registry)
        try:
            import_module(module)
        except:
            # Reset the model registry to the state before the last import as
            # this import will have to reoccur on the next request and this
            # could raise NotRegistered and AlreadyRegistered exceptions
            translator._registry = before_import_registry

            # Decide whether to bubble up this error. If the app just
            # doesn't have an translation module, we can ignore the error
            # attempting to import it, otherwise we want it to bubble up.
            if module_has_submodule(mod, 'translation'):
                raise

    for module in TRANSLATION_FILES:
        import_module(module)

    # In debug mode, print a list of registered models and pid to stdout.
    # Note: Differing model order is fine, we don't rely on a particular
    # order, as far as base classes are registered before subclasses.
    if DEBUG:
        try:
            if sys.argv[1] in ('runserver', 'runserver_plus'):
                models = translator.get_registered_models()
                names = ', '.join(m.__name__ for m in models)
                print(
                    'modeltranslation: Registered %d models for translation'
                    ' (%s) [pid: %d].' % (len(models), names, os.getpid())
                )
        except IndexError:
            pass


def handle_translation_registrations(*args, **kwargs):
    """
    Ensures that any configuration of the TranslationOption(s) are handled when
    importing modeltranslation.

    This makes it possible for scripts/management commands that affect models
    but know nothing of modeltranslation.
    """
    from modeltranslation.settings import ENABLE_REGISTRATIONS

    if not ENABLE_REGISTRATIONS:
        # If the user really wants to disable this, they can, possibly at their
        # own expense. This is generally only required in cases where other
        # apps generate import errors and requires extra work on the user's
        # part to make things work.
        return

    # Trigger autodiscover, causing any TranslationOption initialization
    # code to execute.
    autodiscover()
