#!/usr/bin/env python
from __future__ import print_function

import logging
from multiprocessing import Process
import shutil
import socket
import sys
import time

import mock
import pytest
from six import StringIO
from six.moves import (
    BaseHTTPServer,
    socketserver as SocketServer,
)
import ticket_parser2 as tp2
import ticket_parser2.low_level
from ticket_parser2.exceptions import (
    BrokenTvmClientSettings,
    NonRetriableException,
    RetriableException,
    TicketParsingException,
    TvmException,
)
import ticket_parser2.unittest as tp2u
from ticket_parser2.mock import (
    MockedTvmClient,
    TvmClientPatcher,
)
import yatest.common as yc
from yatest.common import network


SRV_TICKET = (
    "3:serv:CBAQ__________9_IgYIexCUkQY:GioCM49Ob6_f80y6FY0XBVN4hLXuMlFeyMvIMiDuQnZkbkLpRp"
    "QOuQo5YjWoBjM0Vf-XqOm8B7xtrvxSYHDD7Q4OatN2l-Iwg7i71lE3scUeD36x47st3nd0OThvtjrFx_D8mw_"
    "c0GT5KcniZlqq1SjhLyAk1b_zJsx8viRAhCU"
)
PROD_TICKET = (
    "3:user:CAsQ__________9_Gg4KAgh7EHsg0oXYzAQoAA:N8PvrDNLh-5JywinxJntLeQGDEHBUxfzjuvB8-_B"
    "EUv1x9CALU7do8irDlDYVeVVDr4AIpR087YPZVzWPAqmnBuRJS0tJXekmDDvrivLnbRrzY4IUXZ_fImB0fJhTy"
    "VetKv6RD11bGqnAJeDpIukBwPTbJc_EMvKDt8V490CJFw"
)
TEST_TICKET = (
    "3:user:CA0Q__________9_Gg4KAgh7EHsg0oXYzAQoAQ:FSADps3wNGm92Vyb1E9IVq5M6ZygdGdt1vafWWEh"
    "fDDeCLoVA-sJesxMl2pGW4OxJ8J1r_MfpG3ZoBk8rLVMHUFrPa6HheTbeXFAWl8quEniauXvKQe4VyrpA1SPgt"
    "RoFqi5upSDIJzEAe1YRJjq1EClQ_slMt8R0kA_JjKUX54"
)
PROD_YATEAM_TICKET = (
    "3:user:CAwQ__________9_Gg4KAgh7EHsg0oXYzAQoAg:JBYQYr71TnozlBiJhGVyCKdAhlDtrEda1ofe4mCz"
    "0OkxWi4J1EtB3CeYUkxSO4iTSAqJVq8bFdneyS7YCVOt4u69E-SClzRgZ6v7A36l4Z25XNovqC-0o1h-IwFTgy"
    "CZfoPJVfkEOmAYXV4YINBca6L2lZ7ux6q0s5Q5_kUnkAk"
)
TEST_YATEAM_TICKET = (
    "3:user:CA4Q__________9_GhIKBAjAxAcQwMQHINKF2MwEKAM:CpRDQBbh5icA3NCuKuSZUIO0gNyWXej1XfI"
    "nEiSvhs6wcrDHCeQbxzYOfeq2wM801DkaebSmnDBgoWjC7C9hMj4xpmOF_QhRfhFibXbm0O-7lbczO8zLL080m"
    "s59rpaEU3SOKLJ-HaaXrjPCIGSTAIJRvWnck-QXJXPpqmPETr8"
)

TVM_RESP = '{"19"  : { "ticket" : "3:serv:CBAQ__________9_IgYIKhCUkQY:CX"}}'.encode('utf-8')

log_stream = StringIO()
logger = logging.getLogger('TVM')
handler = logging.StreamHandler(stream=log_stream)
handler.setLevel(logging.DEBUG)
logger.addHandler(handler)


def get_log_stream_value():
    return log_stream.getvalue().lstrip('\x00')


def test_settings():
    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmApiClientSettings(self_client_id=0)

    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmApiClientSettings(enable_service_ticket_checking=True)
    tp2.TvmApiClientSettings(enable_service_ticket_checking=True, self_client_id=123)

    tp2.TvmApiClientSettings(enable_user_ticket_checking=tp2.BlackboxEnv.Test)

    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmApiClientSettings()
    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmApiClientSettings(self_secret='asd', dsts={'qwe': 1})
    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmApiClientSettings(self_secret='', dsts={'qwe': 1})
    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmApiClientSettings(self_secret='asd', dsts={})
    with pytest.raises(TvmException):
        tp2.TvmApiClientSettings(self_secret='asd', dsts='kek', self_client_id=123)
    tp2.TvmApiClientSettings(self_secret='asd', dsts={'qwe': 1}, self_client_id=123)

    s = tp2.TvmApiClientSettings(enable_user_ticket_checking=tp2.BlackboxEnv.Test)
    s.set_disk_cache_dir('./')

    with pytest.raises(BrokenTvmClientSettings):
        tp2.TvmClient('kek')


def test_full_client():
    path = yc.source_path() + '/library/cpp/tvmauth/client/ut/files/'
    shutil.copyfile(path + 'public_keys', './public_keys')
    shutil.copyfile(path + 'service_tickets', './service_tickets')

    c = None
    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            self_client_id=100500,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
            self_secret='qwerty',
            dsts={'dest': 19},
        )
        s.set_disk_cache_dir('./')

        c = tp2.TvmClient(s)
        time.sleep(1)

        exp = "File './service_tickets' was successfully read\n"
        exp += "Got 1 service ticket(s) from disk\n"
        exp += "Cache was updated with 1 service ticket(s): 2050-01-01T00:00:00.000000Z\n"
        exp += "File './public_keys' was successfully read\n"
        exp += "Cache was updated with public keys: 2050-01-01T00:00:00.000000Z\n"
        exp += "File './retry_settings' does not exist\n"
        exp += "Thread-worker started\n"
        assert exp == get_log_stream_value()

        st = c.status
        assert st == tp2.TvmClientStatus.Ok
        assert tp2.TvmClient.status_to_string(st) == 'TvmClient cache is ok'

        #    assert c.last_update_time_of_public_keys == datetime.strptime('2050-01-01 03:00:00', '%Y-%m-%d %H:%M:%S')
        #    assert c.last_update_time_of_service_tickets == datetime.strptime('2050-01-01 03:00:01', '%Y-%m-%d %H:%M:%S')

        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for('dest')
        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(alias='dest')
        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(client_id=19)
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for('dest2')
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for(client_id=20)
        with pytest.raises(TvmException):
            c.get_service_ticket_for()

        assert c.check_service_ticket(SRV_TICKET)
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(PROD_TICKET)
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(TEST_TICKET)

        assert c.check_user_ticket(TEST_TICKET)
        with pytest.raises(TicketParsingException):
            c.check_user_ticket(PROD_TICKET)
        with pytest.raises(TicketParsingException):
            c.check_user_ticket(SRV_TICKET)

        with pytest.raises(TicketParsingException):
            assert c.check_user_ticket(TEST_TICKET, overrided_bb_env=tp2.BlackboxEnv.Prod)
        c.check_user_ticket(PROD_TICKET, overrided_bb_env=tp2.BlackboxEnv.Prod)

    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        print('==test_full_client: 1')
        if c is not None:
            c.stop()
        print('==test_full_client: 2')


def test_getting_client_without_aliases():
    path = yc.source_path() + '/library/cpp/tvmauth/client/ut/files/'
    shutil.copyfile(path + 'public_keys', './public_keys')
    shutil.copyfile(path + 'service_tickets', './service_tickets')

    c = None
    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            self_client_id=100500,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
            self_secret='qwerty',
            dsts=[19],
        )
        s.set_disk_cache_dir('./')

        c = tp2.TvmClient(s)
        time.sleep(1)

        exp = "File './service_tickets' was successfully read\n"
        exp += "Got 1 service ticket(s) from disk\n"
        exp += "Cache was updated with 1 service ticket(s): 2050-01-01T00:00:00.000000Z\n"
        exp += "File './public_keys' was successfully read\n"
        exp += "Cache was updated with public keys: 2050-01-01T00:00:00.000000Z\n"
        exp += "File './retry_settings' does not exist\n"
        exp += "Thread-worker started\n"
        assert exp == get_log_stream_value()

        st = c.status
        assert st == tp2.TvmClientStatus.Ok
        assert tp2.TvmClient.status_to_string(st) == 'TvmClient cache is ok'

        #    assert c.last_update_time_of_public_keys == datetime.strptime('2050-01-01 03:00:00', '%Y-%m-%d %H:%M:%S')
        #    assert c.last_update_time_of_service_tickets == datetime.strptime('2050-01-01 03:00:01', '%Y-%m-%d %H:%M:%S')

        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(client_id=19)
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for(client_id=20)

        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for('dest')
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for(alias='dest')
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for('dest2')

    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        print('==test_getting_client_without_aliases: 1')
        if c is not None:
            c.stop()
        print('==test_getting_client_without_aliases: 2')


def test_checking_client():
    path = yc.source_path() + '/library/cpp/tvmauth/client/ut/files/'
    shutil.copyfile(path + 'public_keys', './public_keys')

    c = None
    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
        )
        s.set_disk_cache_dir('./')
        c = tp2.TvmClient(s)
        assert c.status == tp2.TvmClientStatus.Ok

        with pytest.raises(BrokenTvmClientSettings):
            c.check_service_ticket(SRV_TICKET)
        assert c.check_user_ticket(TEST_TICKET)

        print('==test_checking_client: 1')
        c.stop()
        print('==test_checking_client: 2')

        s = tp2.TvmApiClientSettings(
            self_client_id=100500,
            enable_service_ticket_checking=True,
        )
        s.set_disk_cache_dir('./')
        c = tp2.TvmClient(s)
        assert c.status == tp2.TvmClientStatus.Ok

        with pytest.raises(BrokenTvmClientSettings):
            c.check_user_ticket(TEST_TICKET)
        assert c.check_service_ticket(SRV_TICKET)

        print('==test_checking_client: 3')
        c.stop()
        print('==test_checking_client: 4')
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        print('==test_checking_client: 5')
        if c is not None:
            c.stop()
        print('==test_checking_client: 6')


class myHTTPServer(SocketServer.ForkingMixIn, BaseHTTPServer.HTTPServer):
    address_family = socket.AF_INET6
    pass


class myHandler(BaseHTTPServer.BaseHTTPRequestHandler):
    def log_message(self, format, *args):
        sys.stdout.write("%s - - [%s] %s\n" % (self.address_string(), self.log_date_time_string(), format % args))


def test_user_bad_api():
    myHandler.log_message
    pm = network.PortManager()
    port = pm.get_tcp_port(8080)
    server = myHTTPServer(('', port), myHandler)
    thread = Process(target=server.serve_forever)
    thread.start()

    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            enable_user_ticket_checking=tp2.BlackboxEnv.Test,
        )
        s.__set_localhost(port)

        with pytest.raises(RetriableException):
            tp2.TvmClient(s)
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        thread.terminate()


def test_service_bad_api():
    pm = network.PortManager()
    port = pm.get_tcp_port(8080)
    server = myHTTPServer(('', port), myHandler)
    thread = Process(target=server.serve_forever)
    thread.start()

    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            self_client_id=100500,
            enable_service_ticket_checking=True,
        )
        s.__set_localhost(port)

        with pytest.raises(RetriableException):
            tp2.TvmClient(s)
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        thread.terminate()


def test_tickets_bad_api():
    pm = network.PortManager()
    port = pm.get_tcp_port(8080)
    server = myHTTPServer(('', port), myHandler)
    thread = Process(target=server.serve_forever)
    thread.start()

    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            self_client_id=100500,
            self_secret='qwerty',
            dsts={'dest': 19},
        )
        s.__set_localhost(port)

        with pytest.raises(RetriableException):
            tp2.TvmClient(s)
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        thread.terminate()


class myGoodHandler(myHandler):
    def do_GET(self):
        if self.path.startswith("/2/keys"):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header('Content-Length', len(tp2u.TVMKNIFE_PUBLIC_KEYS))
            self.end_headers()
            self.wfile.write(tp2u.TVMKNIFE_PUBLIC_KEYS.encode('utf-8'))
            return

        self.send_error(404, 'Not Found: %s' % self.path)

    def do_POST(self):
        if self.path.startswith("/2/ticket"):

            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-Length', len(TVM_RESP))
            self.end_headers()
            self.wfile.write(TVM_RESP)
            return

        self.send_error(404, 'Not Found: %s' % self.path)


def test_ok_api():
    pm = network.PortManager()
    port = pm.get_tcp_port(8080)
    server = myHTTPServer(('', port), myGoodHandler)
    thread = Process(target=server.serve_forever)
    thread.start()

    c = None
    log_stream.truncate(0)
    try:
        s = tp2.TvmApiClientSettings(
            self_client_id=100500,
            enable_service_ticket_checking=True,
            self_secret='qwerty',
            dsts={'dest': 19},
        )
        s.__set_localhost(port)

        c = tp2.TvmClient(s)

        time.sleep(1)
        assert c.status == tp2.TvmClientStatus.Ok

        actual_log = get_log_stream_value()
        assert actual_log.count('Thread-worker started') == 1

        print('==test_ok_api: 1')
        c.stop()
        print('==test_ok_api: 2')

        with pytest.raises(NonRetriableException):
            c.status
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        thread.terminate()
        if c is not None:
            c.stop()


AUTH_TOKEN = 'some string'
META = """{
"bb_env" : "ProdYaTeam",
"tenants" : [
    {
        "self": {
            "alias" : "me",
            "client_id": 100500
        },
        "dsts" : [
            {
                "alias" : "bbox",
                "client_id": 242
            },
            {
                "alias" : "pass_likers",
                "client_id": 11
            }
        ]
    },
    {
        "self": {
            "alias" : "push-client",
            "client_id": 100501
        },
        "dsts" : [
            {
                "alias" : "pass_likers",
                "client_id": 100502
            }
        ]
    },
    {
        "self": {
            "alias" : "something_else",
            "client_id": 100503
        },
        "dsts" : [
        ]
    }
]
}""".encode(
    'utf-8'
)
TICKETS_ME = """{
    "pass_likers": {
        "ticket": "3:serv:CBAQ__________9_IgYIKhCUkQY:CX",
        "tvm_id": 11
    },
    "bbox": {
        "ticket": "3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc",
        "tvm_id": 242
    }
}""".encode(  # noqa
    'utf-8'
)
BIRTH_TIME = 14380887840


class tvmtoolGoodHandler(myHandler):
    def do_GET(self):
        if self.path.startswith("/tvm/ping"):
            self.send_response(200)
            self.end_headers()
            self.wfile.write("OK".encode('utf-8'))
            return

        if self.headers.get('Authorization', '') != AUTH_TOKEN:
            self.send_error(401, 'Unauthorized')
            return

        if self.path.startswith("/tvm/keys"):
            self.send_response(200)
            self.send_header('Content-type', 'text/plain')
            self.send_header('Content-Length', len(tp2u.TVMKNIFE_PUBLIC_KEYS))
            self.send_header('X-Ya-Tvmtool-Data-Birthtime', BIRTH_TIME)
            self.end_headers()
            self.wfile.write(tp2u.TVMKNIFE_PUBLIC_KEYS.encode('utf-8'))
            return

        if self.path.startswith("/tvm/tickets"):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-Length', len(TICKETS_ME))
            self.send_header('X-Ya-Tvmtool-Data-Birthtime', BIRTH_TIME)
            self.end_headers()
            self.wfile.write(TICKETS_ME)
            return

        if self.path.startswith("/tvm/private_api/__meta__"):
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Content-Length', len(META))
            self.end_headers()
            self.wfile.write(META)
            return

        self.send_error(404, 'Not Found: %s' % self.path)


def test_bad_tool():
    pm = network.PortManager()
    port = pm.get_tcp_port(8080)
    server = myHTTPServer(('', port), tvmtoolGoodHandler)
    thread = Process(target=server.serve_forever)
    thread.start()

    log_stream.truncate(0)
    try:
        s = tp2.TvmToolClientSettings(
            self_alias='no one',
            auth_token=AUTH_TOKEN,
            port=port,
        )

        print("=====test_bad_tool 01")
        with pytest.raises(NonRetriableException):
            tp2.TvmClient(s)
        print("=====test_bad_tool 02")

        exp = "Meta info fetched from localhost:%d\n" % port
        assert get_log_stream_value() == exp
        log_stream.truncate(0)

        s = tp2.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            port=0,
        )

        with pytest.raises(NonRetriableException):
            tp2.TvmClient(s)

        s = tp2.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            hostname='::1',
            port=port,
            override_bb_env=tp2.BlackboxEnv.Stress,
        )

        assert get_log_stream_value() == ''

        with pytest.raises(BrokenTvmClientSettings):
            tp2.TvmClient(s)

        exp = "Meta info fetched from ::1:%d\n" % port
        exp += "Meta: self_tvm_id=100500, bb_env=ProdYateam, idm_slug=<NULL>, dsts=[(pass_likers:11)(bbox:242)]\n"
        assert get_log_stream_value() == exp
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        thread.terminate()


def test_ok_tool():
    pm = network.PortManager()
    port = pm.get_tcp_port(8080)
    server = myHTTPServer(('', port), tvmtoolGoodHandler)
    thread = Process(target=server.serve_forever)
    thread.start()

    log_stream.truncate(0)
    c = None
    try:
        s = tp2.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            port=port,
        )

        c = tp2.TvmClient(s)

        assert c.check_service_ticket(SRV_TICKET)
        assert c.check_user_ticket(PROD_YATEAM_TICKET)
        with pytest.raises(TvmException):
            c.check_user_ticket(TEST_YATEAM_TICKET)

        assert c.status == tp2.TvmClientStatus.Ok

        assert (
            '3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc'  # noqa
            == c.get_service_ticket_for('bbox')
        )
        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(client_id=11)

        c.stop()

        exp = "Meta info fetched from localhost:%d\n" % port
        exp += "Meta: self_tvm_id=100500, bb_env=ProdYateam, idm_slug=<NULL>, dsts=[(pass_likers:11)(bbox:242)]\n"
        exp += "Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
        exp += "Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
        exp += "Thread-worker started\n"
        exp += "Thread-worker stopped\n"
        assert get_log_stream_value() == exp

        s = tp2.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            port=port,
            override_bb_env=tp2.BlackboxEnv.Prod,
        )

        c = tp2.TvmClient(s)

        assert c.check_service_ticket(SRV_TICKET)
        assert c.check_user_ticket(PROD_TICKET)
        with pytest.raises(TvmException):
            c.check_user_ticket(TEST_TICKET)

        c.stop()
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        thread.terminate()
        print('==test_ok_tool: 1')
        if c is not None:
            c.stop()
        print('==test_ok_tool: 2')


def test_fake_mock():
    fake_tvm_client = mock.Mock()
    with TvmClientPatcher(fake_tvm_client):
        fake_tvm_client.get_service_ticket_for.return_value = 'ololo'
        assert 'ololo' == tp2.TvmClient().get_service_ticket_for()
        fake_tvm_client.check_service_ticket.return_value = ticket_parser2.low_level.ServiceContext(
            100500, 'qwerty', tp2u.TVMKNIFE_PUBLIC_KEYS
        ).check(SRV_TICKET)
        assert 123 == tp2.TvmClient().check_service_ticket('').src

    with TvmClientPatcher(MockedTvmClient()) as p:
        p.get_mocked_tvm_client().check_service_ticket = mock.Mock(
            side_effect=TicketParsingException("Unsupported version", tp2.Status.UnsupportedVersion, "2:err"),
        )

        c = tp2.TvmClient()
        assert tp2.TvmClientStatus.Ok == c.status
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(SRV_TICKET)

    m = MockedTvmClient()
    m.get_service_ticket_for = mock.Mock(
        side_effect=['SERVICE_TICKET_FOR_MY_FIRST_CALL', 'SERVICE_TICKET_FOR_MY_SECOND_CALL'],
    )
    with TvmClientPatcher(m):
        c = tp2.TvmClient()
        assert tp2.TvmClientStatus.Ok == c.status
        assert 'SERVICE_TICKET_FOR_MY_FIRST_CALL' == c.get_service_ticket_for()
        assert 'SERVICE_TICKET_FOR_MY_SECOND_CALL' == c.get_service_ticket_for()


def test_default_mock():
    with TvmClientPatcher():
        c = tp2.TvmClient()
        assert tp2.TvmClientStatus.Ok == c.status
        assert 123 == c.check_service_ticket(SRV_TICKET).src
        assert 123 == c.check_user_ticket(TEST_TICKET).default_uid
        assert 'Some service ticket' == c.get_service_ticket_for("foo")
        assert 'TvmClient cache is ok' == c.status_to_string(c.status)

        c.stop()
        with pytest.raises(NonRetriableException):
            c.status
        with pytest.raises(NonRetriableException):
            c.check_service_ticket(SRV_TICKET)
        with pytest.raises(NonRetriableException):
            c.check_user_ticket(TEST_TICKET)
        with pytest.raises(NonRetriableException):
            c.get_service_ticket_for("foo")


def test_mock():
    with TvmClientPatcher(MockedTvmClient(self_client_id=100501)):
        c = tp2.TvmClient()
        assert tp2.TvmClientStatus.Ok == c.status
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(SRV_TICKET)
        assert 123 == c.check_user_ticket(TEST_TICKET).default_uid
        assert 'Some service ticket' == c.get_service_ticket_for("foo")
