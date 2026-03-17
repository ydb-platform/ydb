from __future__ import absolute_import
from django.utils.version import get_complete_version
from django.contrib.auth.backends import RemoteUserBackend
from django_webtest.compat import from_wsgi_safe_string

class WebtestUserBackend(RemoteUserBackend):
    """ Auth backend for django-webtest auth system """

    # Although it appears that we're just creating work for ourselves here
    # because we're calling super(WebtestUserBackend, self).authenticate with
    # unchanged arguments, what matters is the name of the arguments.
    # Specifically, django.contrib.auth.authenticate looks at the names of the
    # arguments of a backend's authenticate method to decide whether the
    # backend can be used to authenticate a particular request.
    if get_complete_version() >= (1, 11):
        def authenticate(self, request, django_webtest_user):
            return super(WebtestUserBackend, self).authenticate(
                request, django_webtest_user)
    else:
        def authenticate(self, django_webtest_user):
            return super(WebtestUserBackend, self).authenticate(
                django_webtest_user)

    def clean_username(self, username):
        return from_wsgi_safe_string(username)


class WebtestUserWithoutPermissionsBackend(WebtestUserBackend):
    """
    Auth backend that passes-through any permission check to further backends
    """

    def get_perm(self, user_obj, perm, obj=None):
        # Indicate that this backend does not handle permissions and
        # allow Django's django.contrib.auth.models._user_has_perm
        # utility to move on to other enabled authentication backends.
        return False
