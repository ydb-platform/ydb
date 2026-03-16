import asyncio

from asgiref.sync import async_to_sync
from rest_framework import exceptions
from rest_framework.request import Request, wrap_attributeerrors


class AsyncRequest(Request):
    @property
    def user(self):
        """
        Returns the user associated with the current request, as authenticated
        by the authentication classes provided to the request.
        """

        if not hasattr(self, "_user"):
            with wrap_attributeerrors():
                self._authenticate()

        return self._user

    @user.setter
    def user(self, value):
        """
        Sets the user on the current request. This is necessary to maintain
        compatibility with django.contrib.auth where the user property is
        set in the login and logout functions.

        Note that we also set the user on Django's underlying `HttpRequest`
        instance, ensuring that it is available to any middleware in the stack.
        """
        self._user = value
        self._request.user = value

    def _authenticate(self):
        """
        Authenticates the user using the available authenticators.

        Raises:
            exceptions.APIException: If an exception occurs during authentication.

        """
        for authenticator in self.authenticators:
            try:
                if asyncio.iscoroutinefunction(authenticator.authenticate):
                    user_auth_tuple = async_to_sync(authenticator.authenticate)(self)
                else:
                    user_auth_tuple = authenticator.authenticate(self)
            except exceptions.APIException:
                self._not_authenticated()
                raise

            if user_auth_tuple is not None:
                self._authenticator = authenticator
                self.user, self.auth = user_auth_tuple
                return

        self._not_authenticated()
