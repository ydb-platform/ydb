from __future__ import print_function

import datetime
import enum
import logging
import threading

import six
import tvmauth.exceptions
from tvmauth.tvmauth_pymodule import (
    __Logger as _Logger,
    __TvmClient as _TvmClient,
    __TvmApiClientSettings as _TvmApiClientSettings,
    __TvmToolClientSettings as _TvmToolClientSettings,
)
from tvmauth.tvmauth_pymodule import __version__  # noqa


__doc__ = 'https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/README.md'


TIROLE_TVMID = 2028120
TIROLE_TVMID_TEST = 2026536


class TicketStatus(enum.IntEnum):
    """
    TicketStatus mean result of ticket check
    """

    # Must be syncronized with __TicketStatus in .pyx
    Ok = 0
    Expired = 1
    InvalidBlackboxEnv = 2
    InvalidDst = 3
    InvalidTicketType = 4
    Malformed = 5
    MissingKey = 6
    SignBroken = 7
    UnsupportedVersion = 8
    NoRoles = 9


class BlackboxEnv(enum.IntEnum):
    """
    BlackboxEnv describes environment of Passport:
    https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#0-opredeljaemsjasokruzhenijami
    """

    Prod = 0
    Test = 1
    ProdYateam = 2
    TestYateam = 3
    Stress = 4


class BlackboxTvmId(enum.Enum):
    Prod = '222'
    Test = '224'
    ProdYateam = '223'
    TestYateam = '225'
    Stress = '226'
    Mimino = '239'


class TvmClientStatus(enum.IntEnum):
    """
    Description:
    https://a.yandex-team.ru/arc/trunk/arcadia/library/python/tvmauth/README.md#high-level-interface
    """

    Ok = 0
    Warn = 1
    Error = 2


class TvmClientStatusExt:
    def __init__(self, code, last_error):
        self._code = code
        self._last_error = last_error

    def __eq__(self, other):
        if isinstance(other, TvmClientStatus):
            return other == self._code
        if isinstance(other, TvmClientStatusExt):
            return other._code == self._code and other._last_error == self._last_error
        raise TypeError('Unsupported type: %s' % type(other))

    @property
    def code(self):
        """
        Use only this prop to make decisions about client status
        """
        return self._code

    @property
    def last_error(self):
        """
        WARNING: this is a text description of some bad event or not.
                 It can be changed in any moment - and it won't be API breaking change.
        """
        return self._last_error


class TvmClient(object):
    """
    Long lived thread-safe object for interacting with TVM.
    Each client starts system thread. !!SO DO NOT FORK YOUR PROCESS AFTER CREATING TvmClient!!
    In 99% cases TvmClient shoud be created at service startup and live for the whole process lifetime.
    If your case in this 1% and you need to RESTART client, you should to use method 'stop()' for old client.

    You can get logs from 'TVM':
        log = logging.getLogger('TVM')
    """

    def __init__(self, settings):
        """
        :param settings: TvmApiClientSettings or TvmToolClientSettings - settings for client

        :raises `NonRetriableException`: Raised in case of settings validation fails.
        :raises `RetriableException`:    Raised if network unavailable.
        """
        if not isinstance(settings, (TvmToolClientSettings, TvmApiClientSettings)):
            raise tvmauth.exceptions.BrokenTvmClientSettings(
                "'settings' must be instance of TvmApiClientSettings or TvmToolClientSettings"
            )

        self.__init_impl(settings)

    def __del__(self):
        self.stop()

    @property
    def status(self):
        """
        :return: Current status of client - :class:`~TvmClientStatus`
        """
        code, last_error = self._ins.status
        return TvmClientStatusExt(code, last_error)

    def get_service_ticket_for(self, alias=None, tvm_id=None):
        """
        Fetching must be enabled in TvmApiClientSettings

        :param alias: string - see docstring for TvmApiClientSettings.__init__
        :param tvm_id: int - any destination you specified in TvmApiClientSettings

        :raises `BrokenTvmClientSettings`: Raised in case of unconfigured using.

        :returns: string - ServiceTicket
        """
        return self._ins.get_service_ticket_for(alias, tvm_id)

    def check_service_ticket(self, ticket):
        """
        :param ticket: string - ticket body

        :raises `BrokenTvmClientSettings`: Raised in case of unconfigured using.
        :raises `TicketParsingException`:  Raised in case of invalid ticket.

        :return: Valid ticket structure
        """
        return CheckedServiceTicket(self._ins.check_service_ticket(ticket))

    def check_user_ticket(self, ticket, overrided_bb_env=None):
        """
        :param ticket: string - ticket body
        :param overrided_bb_env: enum EBlackboxEnv

        :raises `BrokenTvmClientSettings`: Raised in case of unconfigured using.
        :raises `TicketParsingException`:  Raised in case of invalid ticket.

        :return: Valid ticket structure
        """
        return CheckedUserTicket(self._ins.check_user_ticket(ticket, overrided_bb_env))

    def get_roles(self):
        return Roles(self._ins.get_roles())

    def stop(self):
        """
        First call will delete object. Next calls will be no-op.
        """
        if hasattr(self, '_task'):
            self._task.terminate()
        if hasattr(self, '_thread'):
            self._thread.join(1.0)
            if self._thread.is_alive():
                logging.getLogger('TVM').warning(
                    "TVM client was not able to stop correctly, please call TVMClient.stop explicitly"
                )
            del self._thread
        if hasattr(self, '_ins'):
            self._ins.stop()
            # do not delete _ins:
            #   it will throw correct exception if it was stopped and used afterwards
        if hasattr(self, '_task'):
            self._task.do()
            del self._task

    def __init_impl(self, settings):
        class __Task:
            def __init__(self, logger):
                self._stop_event = threading.Event()
                self._logger = logger

                log = logging.getLogger('TVM')
                self._loghandles = {
                    0: log.error,
                    1: log.error,
                    2: log.error,
                    3: log.error,
                    4: log.warning,
                    5: log.info,
                    6: log.info,
                    7: log.debug,
                }

            def terminate(self):
                self._stop_event.set()

            def do(self):
                for lvl, msg in self._logger.fetch():
                    self._loghandles[lvl](msg)

            def run(self):
                while not self._stop_event.wait(0.5):
                    self.do()

        logger = _Logger()
        self._task = __Task(logger)

        try:
            self._ins = _TvmClient(settings._ins, logger)
        except Exception:
            raise
        finally:
            self._task.do()

        self._thread = threading.Thread(target=self._task.run)
        self._thread.daemon = True
        self._thread.start()


class TvmToolClientSettings(object):
    """
    Uses local http-interface to get state: http://localhost/tvm/.
    This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
    See more: https://wiki.yandex-team.ru/passport/tvm2/qloud/.
    """

    def __init__(
        self,
        self_alias,
        auth_token=None,
        port=None,
        hostname="localhost",
        override_bb_env=None,
        check_src_by_default=True,
        check_default_uid_by_default=True,
    ):
        """
        Examples:
        - Ctor for Qloud:
            TvmToolClientSettings("me") # 'me' was specified as alias for your tvm tvm_id in Yandex.Deploy interface
        - Ctor for local tvmtool:
            TvmToolClientSettings("me", auth_token="AAAAAAAAAAAAAAAAAAAAAA", port=18080)
        - Ctor for remote tvmtool - in dev-environment (if you need this):
            TvmToolClientSettings("me", auth_token="AAAAAAAAAAAAAAAAAAAAAA", port=18080, hostname="front.dev.yandex.net")
        - Get ticket from client:
            c = TvmClient(TvmToolClientSettings("me"))  # 'me' was specified as alias for your tvm client in Yandex.Deploy interface
            t = c.get_service_ticket_for("push-client") # 'push-client' was specified as alias for dst in Yandex.Deploy interface
            t = c.get_service_ticket_for(100500)        # 100500 was specified as dst in Yandex.Deploy interface
        - Check user ticket for another bb_env:
            TvmToolClientSettings("me", override_bb_env=BlackboxEnv.ProdYateam)  # BlackboxEnv.Prod was specified for tvmtool

        :param self_alias:      string - alias for your tvm_id.
                                Needs to be specified in settings of tvmtool/Yandex.Deploy
        :param auth_token:      string - it is protection from SSRF.
                                Default value == env['TVMTOOL_LOCAL_AUTHTOKEN'] (provided with Yandex.Deploy)
                                 or env['QLOUD_TVM_TOKEN'] (provided with Qloud)
        :param port:            int - TCP port for HTTP-interface provided with tvmtool/Yandex.Deploy.
                                Default value from env['DEPLOY_TVM_TOOL_URL'] (provided with Yandex.Deploy),
                                 otherwise port == 1 (it is ok for Qloud).
        :param hostname:        string - hostname for tvmtool
        :param override_bb_env: enum EBlackboxEnv - blackbox enviroment overrides env from tvmtool. Allowed only:
                                - Prod/ProdYateam -> Prod/ProdYateam
                                - Test/TestYateam -> Test/TestYateam
                                - Stress -> Stress
                                You can contact tvm-dev@yandex-team.ru if limitations are too strict
        :param check_src_by_default:           boolean - should TvmClient check src in ServiceTickets - it does only binary check:
                                               CheckedServiceTicket gets status NoRoles, if there is no role for src.
                                               If you have a non-binary role system you need to check exact role yourself.
        :param check_default_uid_by_default:   boolean - should TvmClient check default_uid in UserTickets - it does only binary check:
                                               CheckedUserTicket gets status NoRoles, if there is no role for default_uid.
                                               If you have a non-binary role system or you need roles for non-default uids,
                                               you need to check exact role yourself.
        """
        self._ins = _TvmToolClientSettings(
            self_alias=self_alias,
            auth_token=auth_token,
            port=port,
            hostname=hostname,
            override_bb_env=override_bb_env,
            check_src_by_default=check_src_by_default,
            check_default_uid_by_default=check_default_uid_by_default,
        )


class TvmApiClientSettings(object):
    """
    Settings for TVM client. Uses https://tvm-api.yandex.net to get state.
    """

    def __init__(
        self,
        self_tvm_id=None,
        enable_service_ticket_checking=False,
        enable_user_ticket_checking=None,
        self_secret=None,
        dsts=None,
        disk_cache_dir=None,
        localhost_port=None,
        tvmapi_url=None,
        tirole_host=None,
        tirole_port=None,
        tirole_tvmid=None,
        fetch_roles_for_idm_system_slug=None,
        check_src_by_default=True,
        check_default_uid_by_default=True,
    ):
        """
        Examples:
        - Checking of ServiceTickets:
            TvmApiClientSettings(self_tvm_id=100500, enable_service_ticket_checking=True)
        - Checking of UserTickets:
            TvmApiClientSettings(enable_user_ticket_checking=BlackboxEnv.Test)
        - Fetching of ServiceTickets (with aliases):
            # init
            s = TvmApiClientSettings(
                  self_tvm_id=100500,
                  self_secret='my_secret',
                  dsts={'my backend': int(config.get_back_tvm_id())},
            )
            ...
            # per request
            service_ticket_for_backend = c.get_service_ticket_for('my_backend')

            # Key in dict is internal ALIAS of destination in your code.
            # It allowes not to bring destination's tvm_id to each calling point.
        - Fetching of ServiceTickets (with tvm_id):
            # init
            s = TvmApiClientSettings(
                  self_tvm_id=100500,
                  self_secret='my_secret',
                  dsts=[42],
            )
            ...
            # per request
            service_ticket_for_backend = c.get_service_ticket_for(42)

        :param self_tvm_id:                    int - tvm_id of your service
        :param enable_service_ticket_checking: boolean - flag for SeviceTicket checking
                                               this option enables fetching of public keys for signature checking
        :param enable_user_ticket_checking:    enum EBlackboxEnv - blackbox enviroment enables UserTicket checking with env
                                               and enables fetching of public keys for signature checking
        :param self_secret:                    string - TVM-secret of your service
        :param dsts:                           dict (string -> int) - map of alias to tvm_id of your destination
                                               or list (int) - tvm_id of your destination
                                               WARNING: It is not way to provide authorization for incoming ServiceTickets!
                                                        It is way only to send your ServiceTickets to your backend!
        :param disk_cache_dir:                 string - directory should exist
                                               Set path to directory for disk cache
                                               Requires read/write permissions. Checks permissions
                                               WARNING: The same directory can be used only:
                                                       - for TVM clients with the same settings
                                                       OR
                                                       - for new client replacing previous - with another config.
                                                       System user must be the same for processes with these clients inside.
                                                       Implementation doesn't provide other scenarios.
        :param localhost_port:                 int - Switch client to use tvm-api on localhost with provided port
        :param tvmapi_url:                     string - Switch client to use tvm-api with custom url: i.e., for proxy

        :param tirole_host:                    string - source of IDM roles
        :param tirole_port:                    int - source of IDM roles
        :param tirole_tvmid:                   int - tvm_id of tirole: look at experimental.py
        :param fetch_roles_for_idm_system_slug: string - unique name of IDM system
        :param check_src_by_default:           boolean - should TvmClient check src in ServiceTickets - it does only binary check:
                                               CheckedServiceTicket gets status NoRoles, if there is no role for src.
                                               If you have a non-binary role system you need to check exact role yourself.
        :param check_default_uid_by_default:   boolean - should TvmClient check default_uid in UserTickets - it does only binary check:
                                               CheckedUserTicket gets status NoRoles, if there is no role for default_uid.
                                               If you have a non-binary role system or you need roles for non-default uids,
                                               you need to check exact role yourself.

        Some guide for tirole options:
        https://wiki.yandex-team.ru/passport/tvm2/tirole/#3.a.ispolzovatbibliotekutvmauth

        :raises `~BrokenTvmClientSettings`:  Raised in case of settings validation fails.
        """

        self._ins = _TvmApiClientSettings(
            self_tvm_id,
            enable_service_ticket_checking,
            enable_user_ticket_checking,
            self_secret,
            dsts,
            disk_cache_dir,
            localhost_port,
            tvmapi_url,
            tirole_host,
            tirole_port,
            tirole_tvmid,
            fetch_roles_for_idm_system_slug,
            check_src_by_default,
            check_default_uid_by_default,
        )


@six.python_2_unicode_compatible
class CheckedServiceTicket(object):
    def __init__(self, ins=None):
        self._ins = ins

    def __str__(self):
        return self.debug_info

    def __repr__(self):
        return str(self)

    def __nonzero__(self):
        return self._ins.__nonzero__()

    @property
    def src(self):
        """
        You should check SrcID by yourself with your ACL.

        :return: ID of request source service
        """
        return self._ins.src

    @property
    def debug_info(self):
        """
        :return: Human readable data for debug purposes
        """
        return self._ins.debug_info()

    @property
    def issuer_uid(self):
        """
        IssuerUID is UID of developer who is debuging something, so he(she) issued ServiceTicket with his(her) ssh-sign:
            it is grant_type=sshkey in tvm-api
            https://wiki.yandex-team.ru/passport/tvm2/debug/#sxoditvapizakrytoeserviceticketami.

        :return: UID or `None`
        """
        return self._ins.issuer_uid


@six.python_2_unicode_compatible
class CheckedUserTicket(object):
    """
    CheckedUserTicket contains only valid users.
    Details: https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#chtoestvusertickete
    """

    def __init__(self, ins=None):
        self._ins = ins

    def __str__(self):
        return self.debug_info

    def __repr__(self):
        return str(self)

    def __nonzero__(self):
        return self._ins.__nonzero__()

    @property
    def default_uid(self):
        """
        Default UID maybe 0

        :return: default user
        """
        return self._ins.default_uid

    @property
    def uids(self):
        """
        UIDs is array of valid users - never empty

        :return: Array of valid users
        """
        return self._ins.uids

    @property
    def scopes(self):
        """
        Scopes is array of scopes inherited from credential - never empty

        :return: Array of scopes
        """
        return self._ins.scopes

    def has_scope(self, scope_name):
        return self._ins.has_scope(scope_name)

    @property
    def debug_info(self):
        """
        :return: Human readable data for debug purposes
        """
        return self._ins.debug_info()


class Roles(object):
    def __init__(self, ins=None):
        self._ins = ins

    @property
    def meta(self):
        r, b, a = self._ins.meta
        return {
            'revision': r,
            'born_time': datetime.datetime.fromtimestamp(b),
            'applied': datetime.datetime.fromtimestamp(a),
        }

    @property
    def raw(self):
        """
        :return: string
        """
        return self._ins.raw

    def get_service_roles(self, checked_ticket):
        """
        :param checked_ticket: CheckedServiceTicket - your consumer

        :return: dict {str->list [dict {str->str}]} - entities by service roles
        """
        return self._ins.get_service_roles(checked_ticket._ins)

    def get_user_roles(self, checked_ticket, selected_uid=None):
        """
        :param checked_ticket: CheckedUserTicket - your consumer, default_uid is used by default
        :param selected_uid: int - uid in CheckedUserTicket (probably not default_uid)

        :return: dict {str->list [dict {str->str}]} - entities by user roles
        """
        return self._ins.get_user_roles(checked_ticket._ins, selected_uid)

    def check_service_role(self, checked_ticket, role, exact_entity=None):
        """
        :param checked_ticket: CheckedServiceTicket - your consumer
        :param role: str - required role for consumer's action
        :param exact_entity: dict {str->str} - consumer must have role for this entity

        :return: bool - has role or not
        :raises `NonRetriableException`: Raised in case of settings validation fails.
        """
        return self._ins.check_service_role(checked_ticket._ins, role, exact_entity)

    def check_user_role(self, checked_ticket, role, selected_uid=None, exact_entity=None):
        """
        :param checked_ticket: CheckedUserTicket - your consumer, default_uid is used by default
        :param role: str - required role for consumer's action
        :param selected_uid: int - uid in CheckedUserTicket (probably not default_uid)
        :param exact_entity: dict {str->str} - consumer must have role for this entity

        :return: bool - has role or not
        """
        return self._ins.check_user_role(checked_ticket._ins, role, selected_uid, exact_entity)
