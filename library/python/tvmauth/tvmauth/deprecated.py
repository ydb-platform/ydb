from tvmauth import (
    CheckedServiceTicket,
    CheckedUserTicket,
)
from tvmauth.tvmauth_pymodule import (
    __ServiceContext,
    __UserContext,
)


class ServiceContext(__ServiceContext):
    """
    WARNING: it is low level API: first of all try use TvmClient.
    Long lived object for keeping client's credentials for TVM
    """

    def __init__(self, *args, **kwargs):
        self.__base_class().__init__(*args, **kwargs)

    def check(self, ticket_body):
        return CheckedServiceTicket(self.__base_class().check(ticket_body))

    def sign(self, timestamp, dst, scopes=None):
        return self.__base_class().sign(timestamp, dst, scopes)

    def __base_class(self):
        return super(ServiceContext, self)


class UserContext(__UserContext):
    """
    WARNING: it is low level API: first of all try use TvmClient.
    Long lived object for keeping client's credentials for TVM
    """

    def __init__(self, *args, **kwargs):
        self.__base_class().__init__(*args, **kwargs)

    def check(self, ticket_body):
        return CheckedUserTicket(self.__base_class().check(ticket_body))

    def __base_class(self):
        return super(UserContext, self)
