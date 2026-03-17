"""
Implementation of per object permissions for Django.
"""

from . import checks  # noqa: F401


def get_version():
    """Return the version string (see pyproject.toml) of the package.

    The value will comply with the [python version specifier format dicted by
    PEP440](https://packaging.python.org/en/latest/specifications/version-specifiers/#version-specifiers)

    Standards for packaging metadata — including version — are defined by PEP 621,
    which specifies how to declare version in pyproject.toml.

    The earlier PEP 396 suggests (but does not mandate) having a __version__
    attribute in __init__.py for the purposes of runtime introspection, but
    it leads to confusion in our development process to define it multiple places.

    PEP 396 has now been revoked, but it is still useful to be able to inspect
    the package version at runtime. This function retains that ability using the
    recommended importlib approach.
    """
    from importlib.metadata import version

    return version("django-guardian")


def monkey_patch_user():
    from django.contrib.auth import get_user_model

    from .utils import evict_obj_perms_cache, get_anonymous_user, get_user_obj_perms_model

    UserObjectPermission = get_user_obj_perms_model()
    User = get_user_model()
    # Prototype User and Group methods
    setattr(User, "get_anonymous", staticmethod(lambda: get_anonymous_user()))
    setattr(User, "add_obj_perm", lambda self, perm, obj: UserObjectPermission.objects.assign_perm(perm, self, obj))
    setattr(User, "del_obj_perm", lambda self, perm, obj: UserObjectPermission.objects.remove_perm(perm, self, obj))
    setattr(User, "evict_obj_perms_cache", evict_obj_perms_cache)


def monkey_patch_group():
    from django.contrib.auth.models import Group

    from .utils import get_group_obj_perms_model

    GroupObjectPermission = get_group_obj_perms_model()
    # Prototype Group methods
    setattr(Group, "add_obj_perm", lambda self, perm, obj: GroupObjectPermission.objects.assign_perm(perm, self, obj))
    setattr(Group, "del_obj_perm", lambda self, perm, obj: GroupObjectPermission.objects.remove_perm(perm, self, obj))
