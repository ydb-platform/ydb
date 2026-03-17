from django.core.checks import Error, Warning, register


@register(deploy=True)
def schema_check(app_configs, **kwargs):
    """ Perform dummy generation and emit warnings/errors as part of Django's check framework """
    from drf_spectacular.drainage import GENERATOR_STATS
    from drf_spectacular.settings import spectacular_settings

    if not spectacular_settings.ENABLE_DJANGO_DEPLOY_CHECK:
        return []

    errors = []
    try:
        with GENERATOR_STATS.silence():
            spectacular_settings.DEFAULT_GENERATOR_CLASS().get_schema(request=None, public=True)
    except Exception as exc:
        errors.append(
            Error(f'Schema generation threw exception "{exc}"', id='drf_spectacular.E001')
        )
    if GENERATOR_STATS:
        for w in GENERATOR_STATS._warn_cache:
            errors.append(Warning(w, id='drf_spectacular.W001'))
        for e in GENERATOR_STATS._error_cache:
            errors.append(Warning(e, id='drf_spectacular.W002'))
    return errors
