# coding: utf-8

# DJANGO IMPORTS
from django.apps import apps
from django.core.checks import Error, Tags, register
from django.core.exceptions import FieldError
from django.db.models import F


def register_checks():
    register(Tags.models)(check_models)


def check_models(app_configs, **kwargs):
    if app_configs is None:
        app_configs = apps.get_app_configs()

    errors = []
    for app_config in app_configs:
        for model in app_config.get_models():
            errors.extend(check_model(model))
    return errors


def check_model(model):
    errors = []
    errors.extend(_check_autocomplete_search_fields(model))
    return errors


def _check_autocomplete_search_fields(model):
    if not hasattr(model, 'autocomplete_search_fields'):
        return []

    # Ensure that autocomplete_search_fields returns a valid list of filters
    # for a QuerySet on that model
    failures = []
    for lookup in model.autocomplete_search_fields():
        try:
            # This only constructs the QuerySet and doesn't actually query the
            # DB, so it's fine for check phase.
            model._default_manager.filter(**{lookup: F('pk')})
        except FieldError:
            failures.append(lookup)

    if not failures:
        return []
    else:
        return [
            Error(
                "Model {app}.{model} returned bad entries for "
                "autocomplete_search_fields: {failures}".format(
                    app=model._meta.app_label,
                    model=model._meta.model_name,
                    failures=",".join(failures)
                ),
                hint="A QuerySet for {model} could not be constructed. Fix "
                     "the autocomplete_search_fields on it to return valid "
                     "lookups.",
                id='grappelli.E001'
            )
        ]
