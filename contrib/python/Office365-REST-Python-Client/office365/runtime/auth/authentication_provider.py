from abc import ABCMeta, abstractmethod


class AuthenticationProvider(object):
    """Base token provider"""

    __metaclass__ = ABCMeta

    @abstractmethod
    def authenticate_request(self, request):
        """

        :type request: office365.runtime.http.request_options.RequestOptions
        """
        pass
