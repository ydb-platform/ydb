from django.conf import settings
from django.conf.urls import handler404, handler500, include
from django.contrib.auth import get_user_model
from django.contrib.auth.models import AnonymousUser, Group, Permission

__all__ = [
    "Group",
    "Permission",
    "AnonymousUser",
    "get_user_model",
    "user_model_label",
    "include",
    "handler404",
    "handler500",
]

# Since get_user_model() causes a circular import if called when app models are
# being loaded, the user_model_label should be used when possible, with calls
# to get_user_model deferred to execution time

user_model_label = getattr(settings, "AUTH_USER_MODEL", "auth.User")


def get_user_model_path() -> str:
    """Return the python path to the user model class.

    Basically, if `AUTH_USER_MODEL` is set in settings it would be returned,
    otherwise `auth.User` is returned.

    Returns:
        Python path to the user model class in the format
            'app_label.ModelName'.
    """
    return getattr(settings, "AUTH_USER_MODEL", "auth.User")


def get_user_permission_full_codename(perm: str) -> str:
    """Get the full codename for the user permission.

    If standard `auth.User` is used, for 'change' perm this would return `auth.change_user`
    and if `myapp.CustomUser` is used it would return `myapp.change_customuser`.

    Returns:
        Full codename for the user permission in the format
            'app_label.<perm>_<usermodulename>'.
    """
    user_model = get_user_model()
    model_name = user_model._meta.model_name
    return "{}.{}_{}".format(user_model._meta.app_label, perm, model_name)


def get_user_permission_codename(perm: str) -> str:
    """Get the codename for the user permission.

    If standard `auth.User` is used, for 'change' perm this would return `change_user`
    and if `myapp.CustomUser` is used, it would return `change_customuser`.

    Returns:
         Codename for the user permission in the format
            `<perm>_<usermodulename>`
    """
    return get_user_permission_full_codename(perm).split(".")[1]
