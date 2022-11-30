from __future__ import absolute_import

import tvmauth as ta
import tvmauth.deprecated as tad
import tvmauth.exceptions as tae
import tvmauth.unittest as tau


try:
    import mock  # noqa
except ImportError:
    import unittest.mock  # noqa


__doc__ = """
Use TvmClientPatcher to replace TvmClient with MockedTvmClient.
MockedTvmClient can check ServiceTickets and UserTickets from `tvmknife unittest`
Read more: https://wiki.yandex-team.ru/passport/tvm2/debug/#tvmknife
Examples are in docstring for TvmClientPatcher.
"""


class MockedTvmClient(object):
    def __init__(
        self,
        status=ta.TvmClientStatusExt(ta.TvmClientStatus.Ok, "OK"),
        self_tvm_id=100500,
        bb_env=ta.BlackboxEnv.Test,
    ):
        self._status = status
        self._serv_ctx = tad.ServiceContext(self_tvm_id, None, tau.TVMKNIFE_PUBLIC_KEYS)
        self._user_ctx = tad.UserContext(bb_env, tau.TVMKNIFE_PUBLIC_KEYS)
        self._stopped = False

    def __check(self):
        if self._stopped:
            raise tae.NonRetriableException("TvmClient is already stopped")

    def stop(self):
        self._stopped = True

    @property
    def status(self):
        self.__check()
        return self._status

    def get_service_ticket_for(self, alias=None, tvm_id=None):
        """
        You can generate any ticket you want with `tvmknife unittest` and override this function with your ticket
        https://wiki.yandex-team.ru/passport/tvm2/debug/
        """
        self.__check()
        if alias is None and tvm_id is None:
            raise tae.TvmException("One of args is required: 'alias' or 'tvm_id'")
        return "Some service ticket"

    def check_service_ticket(self, ticket):
        self.__check()
        return self._serv_ctx.check(ticket)

    def check_user_ticket(self, ticket):
        self.__check()
        return self._user_ctx.check(ticket)


class TvmClientPatcher(object):
    """
    Example:
    with TvmClientPatcher():
        c = TvmClient()
        assert TvmClientStatus.Ok == c.status
        assert 123 == c.check_service_ticket(SRV_TICKET).src
        assert 123 == c.check_user_ticket(USER_TICKET_TEST).default_uid
        assert 'Some service ticket' == c.get_service_ticket_for("foo")

    Example:
    with TvmClientPatcher(MockedTvmClient(self_tvm_id=100501)):
        c = TvmClient()
        assert TvmClientStatus.Ok == c.status
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(SRV_TICKET)
        assert 123 == c.check_user_ticket(TEST_TICKET).default_uid
        assert 'Some service ticket' == c.get_service_ticket_for("foo")

    Example:
    with TvmClientPatcher(MockedTvmClient()) as p:
        p.get_mocked_tvm_client().check_service_ticket = mock.Mock(
            side_effect=TicketParsingException("Unsupported version", Status.UnsupportedVersion, "2:err"),
        )

        c = TvmClient()
        assert TvmClientStatus.Ok == c.status
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(SRV_TICKET)

    Example:
    m = MockedTvmClient()
    m.get_service_ticket_for = mock.Mock(side_effect=[
        'SERVICE_TICKET_FOR_MY_FIRST_CALL',
        'SERVICE_TICKET_FOR_MY_SECOND_CALL'],
    )
    with TvmClientPatcher(m):
        c = TvmClient()
        assert TvmClientStatus.Ok == c.status
        assert 'SERVICE_TICKET_FOR_MY_FIRST_CALL' == c.get_service_ticket_for()
        assert 'SERVICE_TICKET_FOR_MY_SECOND_CALL' == c.get_service_ticket_for()
    """

    def __init__(self, mocked_tvm_client=None):
        if mocked_tvm_client is None:
            mocked_tvm_client = MockedTvmClient()
        self._mocked_tvm_client = mocked_tvm_client
        self._patch = mock.patch.object(
            ta.TvmClient,
            '__new__',
            mock.Mock(return_value=mocked_tvm_client),
        )

    def start(self):
        self._patch.start()
        return self

    def stop(self):
        self._patch.stop()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.stop()

    def get_mocked_tvm_client(self):
        return self._mocked_tvm_client
