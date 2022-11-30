from __future__ import absolute_import

try:
    import mock  # noqa
except ImportError:
    import unittest.mock  # noqa

try:
    import ticket_parser2_py3 as tp2  # noqa
    import ticket_parser2_py3.low_level as tp2l  # noqa
    import ticket_parser2_py3.exceptions as tp2e  # noqa
    import ticket_parser2_py3.unittest as tp2u  # noqa
except ImportError:
    import ticket_parser2 as tp2  # noqa
    import ticket_parser2.low_level as tp2l  # noqa
    import ticket_parser2.exceptions as tp2e  # noqa
    import ticket_parser2.unittest as tp2u  # noqa


__doc__ = """
Use TvmClientPatcher to replace TvmClient with MockedTvmClient.
MockedTvmClient can check ServiceTickets and UserTickets from `tvmknife unittest`
Read more: https://wiki.yandex-team.ru/passport/tvm2/debug/#tvmknife
Examples are in docstring for TvmClientPatcher.
"""


PUBLIC_KEYS = tp2u.TVMKNIFE_PUBLIC_KEYS


class MockedTvmClient(object):
    def __init__(self, status=tp2.TvmClientStatus.Ok, self_client_id=100500, bb_env=tp2.BlackboxEnv.Test):
        self._status = status
        self._serv_ctx = tp2l.ServiceContext(self_client_id, None, PUBLIC_KEYS)
        self._user_ctx = tp2l.UserContext(bb_env, PUBLIC_KEYS)
        self._stopped = False

    def __check(self):
        if self._stopped:
            raise tp2e.NonRetriableException("TvmClient is already stopped")

    def stop(self):
        self._stopped = True

    @property
    def status(self):
        self.__check()
        return self._status

    @staticmethod
    def status_to_string(status):
        return tp2.TvmClient.status_to_string(status)

    def get_service_ticket_for(self, alias=None, client_id=None):
        """
        You can generate any ticket you want with `tvmknife unittest` and override this function with your ticket
        https://wiki.yandex-team.ru/passport/tvm2/debug/
        """
        self.__check()
        if alias is None and client_id is None:
            raise tp2e.TvmException("One of args is required: 'alias' or 'client_id'")
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
    with TvmClientPatcher(MockedTvmClient(self_client_id=100501)):
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
            tp2.TvmClient,
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
