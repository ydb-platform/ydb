#!/usr/bin/env python
from __future__ import print_function

import datetime
import logging
from multiprocessing import Process
import os
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
import tvmauth
import tvmauth.deprecated
from tvmauth.exceptions import (
    BrokenTvmClientSettings,
    NonRetriableException,
    PermissionDenied,
    RetriableException,
    TicketParsingException,
    TvmException,
)
from tvmauth.mock import (
    MockedTvmClient,
    TvmClientPatcher,
)
import tvmauth.unittest as tp2u
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
        tvmauth.TvmApiClientSettings(self_tvm_id=0)

    with pytest.raises(BrokenTvmClientSettings):
        tvmauth.TvmApiClientSettings(enable_service_ticket_checking=True)
    tvmauth.TvmApiClientSettings(enable_service_ticket_checking=True, self_tvm_id=123)

    tvmauth.TvmApiClientSettings(enable_user_ticket_checking=tvmauth.BlackboxEnv.Test)

    with pytest.raises(BrokenTvmClientSettings):
        tvmauth.TvmApiClientSettings()
    with pytest.raises(BrokenTvmClientSettings):
        tvmauth.TvmApiClientSettings(self_secret='asd', dsts={'qwe': 1})
    with pytest.raises(BrokenTvmClientSettings):
        tvmauth.TvmApiClientSettings(self_secret='', dsts={'qwe': 1})
    with pytest.raises(BrokenTvmClientSettings):
        tvmauth.TvmApiClientSettings(self_secret='asd', dsts={})
    with pytest.raises(TvmException):
        tvmauth.TvmApiClientSettings(self_secret='asd', dsts='kek', self_tvm_id=123)
    tvmauth.TvmApiClientSettings(self_secret='asd', dsts={'qwe': 1}, self_tvm_id=123)

    tvmauth.TvmApiClientSettings(enable_user_ticket_checking=tvmauth.BlackboxEnv.Test)
    with pytest.raises(PermissionDenied):
        tvmauth.TvmApiClientSettings(
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            disk_cache_dir='/',
        )
    tvmauth.TvmApiClientSettings(
        enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
        disk_cache_dir='./',
    )

    with pytest.raises(BrokenTvmClientSettings):
        tvmauth.TvmClient('kek')


def test_full_client():
    path = yc.source_path() + '/library/cpp/tvmauth/client/ut/files/'
    shutil.copyfile(path + 'public_keys', './public_keys')
    shutil.copyfile(path + 'service_tickets', './service_tickets')

    c = None
    log_stream.truncate(0)
    try:
        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            self_secret='qwerty',
            dsts={'dest': 19},
            disk_cache_dir='./',
        )
        c = tvmauth.TvmClient(s)
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
        assert st == tvmauth.TvmClientStatus.Ok

        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for('dest')
        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(alias='dest')
        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(tvm_id=19)
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for('dest2')
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for(tvm_id=20)
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
            assert c.check_user_ticket(TEST_TICKET, overrided_bb_env=tvmauth.BlackboxEnv.Prod)
        c.check_user_ticket(PROD_TICKET, overrided_bb_env=tvmauth.BlackboxEnv.Prod)

    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        print('==test_full_client: 1')
        if c is not None:
            c.stop()
        print('==test_full_client: 2')


def test_client_with_roles():
    os.environ['TZ'] = 'Europe/Moscow'
    time.tzset()

    path = yc.source_path() + '/library/cpp/tvmauth/client/ut/files/'
    shutil.copyfile(path + 'service_tickets', './service_tickets')
    shutil.copyfile(path + 'roles', './roles')

    c = None
    log_stream.truncate(0)
    try:
        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            self_secret='qwerty',
            dsts={'dest': 19},
            disk_cache_dir='./',
            tirole_host='localhost',
            tirole_port=1,
            tirole_tvmid=19,
            fetch_roles_for_idm_system_slug='femida',
        )
        c = tvmauth.TvmClient(s)
        time.sleep(1)

        exp = "File './service_tickets' was successfully read\n"
        exp += "Got 1 service ticket(s) from disk\n"
        exp += "Cache was updated with 1 service ticket(s): 2050-01-01T00:00:00.000000Z\n"
        exp += "File './retry_settings' does not exist\n"
        exp += "File './roles' was successfully read\n"
        exp += "Succeed to read roles with revision 100501 from ./roles\n"
        exp += "Thread-worker started\n"
        assert exp == get_log_stream_value()

        st = c.status
        assert st == tvmauth.TvmClientStatus.Ok

        roles = c.get_roles()
        applied = roles.meta['applied']
        assert roles.meta == {
            'applied': applied,
            'born_time': datetime.datetime(1970, 1, 1, 3, 0, 42),
            'revision': '100501',
        }

        assert roles.get_service_roles(tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501)) == {
            "role#1": [{"attr#1": "val#1"}],
            "role#2": [{"attr#1": "val#2"}],
        }

        assert roles.get_service_roles(tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100502)) == {}

        assert roles.get_user_roles(
            tp2u.create_user_ticket_for_unittest(tvmauth.TicketStatus.Ok, 10005001, env=tvmauth.BlackboxEnv.ProdYateam),
        ) == {
            "role#3": [{"attr#3": "val#3"}],
            "role#4": [{"attr#3": "val#4"}],
            "role#5": [{"attr#3": "val#4", "attr#5": "val#5"}],
        }

        assert (
            roles.get_user_roles(
                tp2u.create_user_ticket_for_unittest(
                    tvmauth.TicketStatus.Ok, 10005002, env=tvmauth.BlackboxEnv.ProdYateam
                ),
            )
            == {}
        )

        with pytest.raises(AttributeError):
            roles.check_service_role(
                tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
                'role#1',
                {"attr#1": 42},
            )

        assert roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
            'role#1',
        )
        assert not roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100502),
            'role#1',
        )
        assert not roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
            'role#42',
        )

        assert roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
            'role#1',
            {"attr#1": "val#1"},
        )
        assert roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
            'role#2',
            {"attr#1": "val#2"},
        )
        assert not roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
            'role#1',
            {"attr#1": "val#2"},
        )
        assert not roles.check_service_role(
            tp2u.create_service_ticket_for_unittest(tvmauth.TicketStatus.Ok, 100501),
            'role#2',
            {"attr#1": "val#1"},
        )

        assert roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(tvmauth.TicketStatus.Ok, 10005001, env=tvmauth.BlackboxEnv.ProdYateam),
            'role#3',
        )
        assert roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(
                tvmauth.TicketStatus.Ok,
                10005000,
                uids=[10005000, 10005001, 10005002],
                env=tvmauth.BlackboxEnv.ProdYateam,
            ),
            'role#3',
            10005001,
        )
        assert not roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(tvmauth.TicketStatus.Ok, 10005002, env=tvmauth.BlackboxEnv.ProdYateam),
            'role#1',
        )
        assert not roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(tvmauth.TicketStatus.Ok, 10005001, env=tvmauth.BlackboxEnv.ProdYateam),
            'role#42',
        )
        assert not roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(
                tvmauth.TicketStatus.Ok,
                10005000,
                uids=[10005000, 10005001, 10005002],
                env=tvmauth.BlackboxEnv.ProdYateam,
            ),
            'role#3',
            10005002,
        )

        assert roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(tvmauth.TicketStatus.Ok, 10005001, env=tvmauth.BlackboxEnv.ProdYateam),
            'role#3',
            exact_entity={"attr#3": "val#3"},
        )
        assert not roles.check_user_role(
            tp2u.create_user_ticket_for_unittest(tvmauth.TicketStatus.Ok, 10005001, env=tvmauth.BlackboxEnv.ProdYateam),
            'role#3',
            exact_entity={"attr#3": "val#4"},
        )
    except Exception:
        print(get_log_stream_value())
        raise
    finally:
        if c is not None:
            c.stop()


def test_getting_client_without_aliases():
    path = yc.source_path() + '/library/cpp/tvmauth/client/ut/files/'
    shutil.copyfile(path + 'public_keys', './public_keys')
    shutil.copyfile(path + 'service_tickets', './service_tickets')

    c = None
    log_stream.truncate(0)
    try:
        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            enable_service_ticket_checking=True,
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            self_secret='qwerty',
            dsts=[19],
            disk_cache_dir='./',
        )

        c = tvmauth.TvmClient(s)
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
        assert st == tvmauth.TvmClientStatus.Ok

        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(tvm_id=19)
        with pytest.raises(BrokenTvmClientSettings):
            c.get_service_ticket_for(tvm_id=20)

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
        s = tvmauth.TvmApiClientSettings(
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            disk_cache_dir='./',
        )
        c = tvmauth.TvmClient(s)
        assert c.status == tvmauth.TvmClientStatus.Ok

        with pytest.raises(BrokenTvmClientSettings):
            c.check_service_ticket(SRV_TICKET)
        assert c.check_user_ticket(TEST_TICKET)

        print('==test_checking_client: 1')
        c.stop()
        print('==test_checking_client: 2')

        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            enable_service_ticket_checking=True,
            disk_cache_dir='./',
        )
        c = tvmauth.TvmClient(s)
        assert c.status == tvmauth.TvmClientStatus.Ok

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
        s = tvmauth.TvmApiClientSettings(
            enable_user_ticket_checking=tvmauth.BlackboxEnv.Test,
            localhost_port=port,
        )

        with pytest.raises(RetriableException):
            tvmauth.TvmClient(s)
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
        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            enable_service_ticket_checking=True,
            localhost_port=port,
        )

        with pytest.raises(RetriableException):
            tvmauth.TvmClient(s)
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
        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            self_secret='qwerty',
            dsts={'dest': 19},
            localhost_port=port,
        )

        with pytest.raises(RetriableException):
            tvmauth.TvmClient(s)
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
        s = tvmauth.TvmApiClientSettings(
            self_tvm_id=100500,
            enable_service_ticket_checking=True,
            self_secret='qwerty',
            dsts={'dest': 19},
            localhost_port=port,
        )

        c = tvmauth.TvmClient(s)

        time.sleep(1)
        assert c.status == tvmauth.TvmClientStatus.Ok

        slept = 0.0
        while get_log_stream_value().count('Thread-worker started') != 1 and slept < 10:
            slept += 0.1
            time.sleep(0.1)
        assert get_log_stream_value().count('Thread-worker started') == 1

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
        s = tvmauth.TvmToolClientSettings(
            self_alias='no one',
            auth_token=AUTH_TOKEN,
            port=port,
        )

        print("=====test_bad_tool 01")
        with pytest.raises(NonRetriableException):
            tvmauth.TvmClient(s)
        print("=====test_bad_tool 02")

        exp = "Meta info fetched from localhost:%d\n" % port
        assert get_log_stream_value() == exp
        log_stream.truncate(0)

        s = tvmauth.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            port=0,
        )

        with pytest.raises(NonRetriableException):
            tvmauth.TvmClient(s)

        s = tvmauth.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            hostname='::1',
            port=port,
            override_bb_env=tvmauth.BlackboxEnv.Stress,
        )

        assert get_log_stream_value() == ''

        with pytest.raises(BrokenTvmClientSettings):
            tvmauth.TvmClient(s)

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
        s = tvmauth.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            port=port,
        )

        c = tvmauth.TvmClient(s)

        assert c.check_service_ticket(SRV_TICKET)
        assert c.check_user_ticket(PROD_YATEAM_TICKET)
        with pytest.raises(TvmException):
            c.check_user_ticket(TEST_YATEAM_TICKET)

        assert c.status == tvmauth.TvmClientStatus.Ok
        assert c.status.code == tvmauth.TvmClientStatus.Ok
        assert c.status.last_error == 'OK'

        assert (
            '3:serv:CBAQ__________9_IgcIlJEGEPIB:N7luw0_rVmBosTTI130jwDbQd0-cMmqJeEl0ma4ZlIo_mHXjBzpOuMQ3A9YagbmOBOt8TZ_gzGvVSegWZkEeB24gM22acw0w-RcHaQKrzSOA5Zq8WLNIC8QUa4_WGTlAsb7R7eC4KTAGgouIquNAgMBdTuGOuZHnMLvZyLnOMKc'  # noqa
            == c.get_service_ticket_for('bbox')
        )
        assert '3:serv:CBAQ__________9_IgYIKhCUkQY:CX' == c.get_service_ticket_for(tvm_id=11)

        c.stop()
        c.stop()

        exp = "Meta info fetched from localhost:%d\n" % port
        exp += "Meta: self_tvm_id=100500, bb_env=ProdYateam, idm_slug=<NULL>, dsts=[(pass_likers:11)(bbox:242)]\n"
        exp += "Tickets fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
        exp += "Public keys fetched from tvmtool: 2425-09-17T11:04:00.000000Z\n"
        exp += "Thread-worker started\n"
        exp += "Thread-worker stopped\n"
        assert get_log_stream_value() == exp

        s = tvmauth.TvmToolClientSettings(
            self_alias='me',
            auth_token=AUTH_TOKEN,
            port=port,
            override_bb_env=tvmauth.BlackboxEnv.Prod,
        )

        c = tvmauth.TvmClient(s)

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
        assert 'ololo' == tvmauth.TvmClient().get_service_ticket_for()
        fake_tvm_client.check_service_ticket.return_value = tvmauth.deprecated.ServiceContext(
            100500, 'qwerty', tp2u.TVMKNIFE_PUBLIC_KEYS
        ).check(SRV_TICKET)
        assert 123 == tvmauth.TvmClient().check_service_ticket('').src

    with TvmClientPatcher(MockedTvmClient()) as p:
        p.get_mocked_tvm_client().check_service_ticket = mock.Mock(
            side_effect=TicketParsingException("Unsupported version", tvmauth.TicketStatus.UnsupportedVersion, "2:err"),
        )

        c = tvmauth.TvmClient()
        assert tvmauth.TvmClientStatus.Ok == c.status
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(SRV_TICKET)

    m = MockedTvmClient()
    m.get_service_ticket_for = mock.Mock(
        side_effect=['SERVICE_TICKET_FOR_MY_FIRST_CALL', 'SERVICE_TICKET_FOR_MY_SECOND_CALL'],
    )
    with TvmClientPatcher(m):
        c = tvmauth.TvmClient()
        assert tvmauth.TvmClientStatus.Ok == c.status
        assert 'SERVICE_TICKET_FOR_MY_FIRST_CALL' == c.get_service_ticket_for()
        assert 'SERVICE_TICKET_FOR_MY_SECOND_CALL' == c.get_service_ticket_for()


def test_default_mock():
    with TvmClientPatcher():
        c = tvmauth.TvmClient()
        assert tvmauth.TvmClientStatus.Ok == c.status
        assert 123 == c.check_service_ticket(SRV_TICKET).src
        assert 123 == c.check_user_ticket(TEST_TICKET).default_uid
        assert 'Some service ticket' == c.get_service_ticket_for("foo")

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
    with TvmClientPatcher(MockedTvmClient(self_tvm_id=100501)):
        c = tvmauth.TvmClient()
        assert tvmauth.TvmClientStatus.Ok == c.status
        with pytest.raises(TicketParsingException):
            c.check_service_ticket(SRV_TICKET)
        assert 123 == c.check_user_ticket(TEST_TICKET).default_uid
        assert 'Some service ticket' == c.get_service_ticket_for("foo")


def test_client_status():
    assert tvmauth.TvmClientStatus.Ok == tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Ok, "kek")
    assert tvmauth.TvmClientStatus.Ok == tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Ok, "kek").code
    assert "kek" == tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Ok, "kek").last_error
    assert tvmauth.TvmClientStatus.Ok != tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Warn, "kek")

    assert tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Warn, "kek") != tvmauth.TvmClientStatusExt(
        tvmauth.TvmClientStatus.Ok, "kek"
    )
    assert tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Ok, "kek1") != tvmauth.TvmClientStatusExt(
        tvmauth.TvmClientStatus.Ok, "kek2"
    )
    assert tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Ok, "kek") == tvmauth.TvmClientStatusExt(
        tvmauth.TvmClientStatus.Ok, "kek"
    )

    with pytest.raises(TypeError):
        tvmauth.TvmClientStatusExt(tvmauth.TvmClientStatus.Ok, "kek") == 42
