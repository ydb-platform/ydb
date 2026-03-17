import json
from urllib.parse import urlunparse

from .settings import scim_settings


def get_user_model():
    """
    Return the user model.
    """
    return scim_settings.USER_MODEL_GETTER()


def get_user_adapter():
    """
    Return the user model adapter.
    """
    return scim_settings.USER_ADAPTER


def get_group_model():
    """
    Return the group model.
    """
    return scim_settings.GROUP_MODEL


def get_group_adapter():
    """
    Return the group model adapter.
    """
    return scim_settings.GROUP_ADAPTER


def get_user_filter_parser():
    """
    Return the user filter parser.
    """
    return scim_settings.USER_FILTER_PARSER


def get_group_filter_parser():
    """
    Return the group filter parser.
    """
    return scim_settings.GROUP_FILTER_PARSER


def get_service_provider_config_model():
    """
    Return the Service Provider Config model.
    """
    return scim_settings.SERVICE_PROVIDER_CONFIG_MODEL


def get_base_scim_location_getter():
    """
    Return a function that will, when called, returns the base
    location of scim app.
    """
    return scim_settings.BASE_LOCATION_GETTER


def get_all_schemas_getter():
    """
    Return a function that will, when called, returns the
    all schemas getter function.
    """
    return scim_settings.SCHEMAS_GETTER


def get_extra_model_filter_kwargs_getter(model):
    """
    Return a function that will, when called, returns the
    get_extra_filter_kwargs function.
    """
    return scim_settings.GET_EXTRA_MODEL_FILTER_KWARGS_GETTER(model)


def get_extra_model_exclude_kwargs_getter(model):
    """
    Return a function that will, when called, returns the
    get_extra_exclude_kwargs function.
    """
    return scim_settings.GET_EXTRA_MODEL_EXCLUDE_KWARGS_GETTER(model)


def get_object_post_processor_getter(model):
    """
    Return a function that will, when called, returns the
    get_object_post_processor function.
    """
    return scim_settings.GET_OBJECT_POST_PROCESSOR_GETTER(model)


def get_queryset_post_processor_getter(model):
    """
    Return a function that will, when called, returns the
    get_queryset_post_processor function.
    """
    return scim_settings.GET_QUERYSET_POST_PROCESSOR_GETTER(model)


def get_is_authenticated_predicate():
    """
    Return function that will perform customized authn/z actions during
    `.dispatch` method processing. This defaults to Django's `user.is_authenticated`.
    """
    return scim_settings.GET_IS_AUTHENTICATED_PREDICATE


def default_is_authenticated_predicate(user):
    return user.is_authenticated


def default_base_scim_location_getter(request=None, *args, **kwargs):
    """
    Return the default location of the app implementing the SCIM api.
    """
    base_scim_location_parts = (
        scim_settings.SCHEME,
        scim_settings.NETLOC,
        '',  # path
        '',  # params
        '',  # query
        ''   # fragment
    )

    base_scim_location = urlunparse(base_scim_location_parts)

    return base_scim_location


def default_get_extra_model_filter_kwargs_getter(model):
    """
    Return a **method** that will return extra model filter kwargs for the passed in model.

    :param model:
    """
    def get_extra_filter_kwargs(request, *args, **kwargs):
        """
        Return extra filter kwargs for the given model.
        :param request:
        :param args:
        :param kwargs:
        :rtype: dict
        """
        return {}

    return get_extra_filter_kwargs


def default_get_extra_model_exclude_kwargs_getter(model):
    """
    Return a **method** that will return extra model exclude kwargs for the passed in model.

    :param model:
    """
    def get_extra_exclude_kwargs(request, *args, **kwargs):
        """
        Return extra exclude kwargs for the given model.
        :param request:
        :param args:
        :param kwargs:
        :rtype: dict
        """
        return {}

    return get_extra_exclude_kwargs


def default_get_object_post_processor_getter(model):
    """
    Return a **method** that can be used to perform any post processing
    on an object about to be returned from SCIMView.get_object().

    :param model:
    """
    def get_object_post_processor(request, obj, *args, **kwargs):
        """
        Perform any post processing on object to be returned from SCIMView.get_object().

        :param request:
        :param obj:
        :param args:
        :param kwargs:
        :rtype: Django User
        """
        return obj

    return get_object_post_processor


def default_get_queryset_post_processor_getter(model):
    """
    Return a **method** that can be used to perform any post processing
    on a queryset about to be returned from GetView.get_many().

    :param model:
    """
    def get_queryset_post_processor(request, qs, *args, **kwargs):
        """
        Perform any post processing on queryset to be returned from GetView.get_many().

        :param request:
        :param qs:
        :param args:
        :param kwargs:
        :rtype: Django Queryset
        """
        return qs

    return get_queryset_post_processor


def clean_structure_of_passwords(obj):
    if isinstance(obj, dict):
        new_obj = {}
        for key, value in obj.items():
            if 'password' in key.lower():
                new_obj[key] = '*' * len(value) if value else None
            else:
                new_obj[key] = clean_structure_of_passwords(value)

        return new_obj

    elif isinstance(obj, list):
        return [clean_structure_of_passwords(item) for item in obj]

    else:
        return obj


def get_loggable_body(text):
    if not text:
        return text

    try:
        obj = json.loads(text)
    except json.JSONDecodeError:
        return text

    obj = clean_structure_of_passwords(obj)

    return json.dumps(obj)
