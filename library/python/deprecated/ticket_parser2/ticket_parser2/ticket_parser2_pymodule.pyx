# coding: utf-8
import datetime
import logging
import time
from cpython cimport PyObject
from libcpp cimport bool as bool_t
from libcpp.map cimport map as cmap
from libcpp.pair cimport pair
from libc.stdint cimport uint32_t, uint64_t
from libc.time cimport time_t
from cython.operator cimport dereference
cimport cpython.ref as cpy_ref

from enum import (
    Enum,
    IntEnum,
)

from util.datetime.base cimport TInstant
from util.generic.hash cimport THashMap
from util.generic.maybe cimport TMaybe
from util.generic.ptr cimport THolder
from util.generic.string cimport TString, TStringBuf
from util.generic.vector cimport TVector


cdef extern from "Python.h":
    cdef void PyEval_InitThreads()

class TvmException(Exception):
    pass

class ContextException(TvmException):
    pass

class EmptyTvmKeysException(ContextException):
    pass

class MalformedTvmKeysException(ContextException):
    pass

class MalformedTvmSecretException(ContextException):
    pass

class NotAllowedException(TvmException):
    pass

class ClientException(TvmException):
    pass

class RetriableException(ClientException):
    pass

class NonRetriableException(ClientException):
    pass

class BrokenTvmClientSettings(NonRetriableException):
    pass

class MissingServiceTicket(NonRetriableException):
    pass

class PermissionDenied(NonRetriableException):
    pass

class TicketParsingException(TvmException):
    def __init__(self, message, status, debug_info):
        self.message = message
        self.status = status
        self.debug_info = debug_info

cdef public PyObject* pyEmptyTvmKeysException = <PyObject*>EmptyTvmKeysException
cdef public PyObject* pyMalformedTvmKeysException = <PyObject*>MalformedTvmKeysException
cdef public PyObject* pyMalformedTvmSecretException = <PyObject*>MalformedTvmSecretException
cdef public PyObject* pyNotAllowedException = <PyObject*>NotAllowedException
cdef public PyObject* pyTicketParsingException = <PyObject*>TicketParsingException
cdef public PyObject* pyClientException = <PyObject*>ClientException
cdef public PyObject* pyBrokenTvmClientSettings = <PyObject*>BrokenTvmClientSettings
cdef public PyObject* pyRetriableException = <PyObject*>RetriableException
cdef public PyObject* pyNonRetriableException = <PyObject*>NonRetriableException
cdef public PyObject* pyMissingServiceTicket = <PyObject*>MissingServiceTicket
cdef public PyObject* pyPermissionDenied = <PyObject*>PermissionDenied


cdef extern from "library/cpp/containers/stack_vector/stack_vec.h" nogil:
    cdef cppclass TSmallVec[T](TVector):
        pass

cdef extern from "library/python/deprecated/ticket_parser2/src/exception.h":
    cdef void raise_py_error()

cdef extern from "library/cpp/tvmauth/ticket_status.h" namespace "NTvmAuth" nogil:
    cdef cppclass EStatus "NTvmAuth::ETicketStatus":
        pass
    TStringBuf StatusToString(EStatus status);

cdef extern from "library/cpp/tvmauth/ticket_status.h" namespace "NTvmAuth::ETicketStatus" nogil:
    cdef EStatus cOk "NTvmAuth::ETicketStatus::Ok"
    cdef EStatus cExpired "NTvmAuth::ETicketStatus::Expired"
    cdef EStatus cInvalidBlackboxEnv "NTvmAuth::ETicketStatus::InvalidBlackboxEnv"
    cdef EStatus cInvalidDst "NTvmAuth::ETicketStatus::InvalidDst"
    cdef EStatus cInvalidTicketType "NTvmAuth::ETicketStatus::InvalidTicketType"
    cdef EStatus cMalformed "NTvmAuth::ETicketStatus::Malformed"
    cdef EStatus cMissingKey "NTvmAuth::ETicketStatus::MissingKey"
    cdef EStatus cSignBroken "NTvmAuth::ETicketStatus::SignBroken"
    cdef EStatus cUnsupportedVersion "NTvmAuth::ETicketStatus::UnsupportedVersion"

class Status(IntEnum):
    """
    Status mean result of ticket check
    """
    Ok = <int>cOk
    Expired = <int>cExpired
    InvalidBlackboxEnv = <int>cInvalidBlackboxEnv
    InvalidDst = <int>cInvalidDst
    InvalidTicketType = <int>cInvalidTicketType
    Malformed = <int>cMalformed
    MissingKey = <int>cMissingKey
    SignBroken = <int>cSignBroken
    UnsupportedVersion = <int>cUnsupportedVersion

cdef extern from "library/cpp/tvmauth/checked_user_ticket.h" namespace "NTvmAuth" nogil:
    cdef cppclass EBlackboxEnv "NTvmAuth::EBlackboxEnv":
        pass

cdef extern from "library/cpp/tvmauth/checked_user_ticket.h" namespace "NTvmAuth::EBlackboxEnv" nogil:
    cdef EBlackboxEnv cProd "NTvmAuth::EBlackboxEnv::Prod"
    cdef EBlackboxEnv cTest "NTvmAuth::EBlackboxEnv::Test"
    cdef EBlackboxEnv cProdYateam "NTvmAuth::EBlackboxEnv::ProdYateam"
    cdef EBlackboxEnv cTestYateam "NTvmAuth::EBlackboxEnv::TestYateam"
    cdef EBlackboxEnv cStress "NTvmAuth::EBlackboxEnv::Stress"

class BlackboxEnv(IntEnum):
    """
    BlackboxEnv describes environment of Passport:
    https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#0-opredeljaemsjasokruzhenijami
    """
    Prod = <int>cProd
    Test = <int>cTest
    ProdYateam = <int>cProdYateam
    TestYateam = <int>cTestYateam
    Stress = <int>cStress

class BlackboxClientId(Enum):
    Prod = '222'
    Test = '224'
    ProdYateam = '223'
    TestYateam = '225'
    Stress = '226'
    Mimino = '239'

cdef extern from "library/cpp/tvmauth/type.h" namespace "NTvmAuth" nogil:
    ctypedef uint32_t TTvmId
    ctypedef uint64_t TUid

    cdef cppclass TScopes(TSmallVec[TStringBuf]):
        pass

    cdef cppclass TUids(TSmallVec[TUid]):
        pass

cdef extern from "library/cpp/tvmauth/src/service_impl.h" namespace "NTvmAuth" nogil:
    cdef cppclass TCheckedServiceTicket:
        cppclass TImpl:
            TImpl() except +raise_py_error
            bool_t operator bool() except +raise_py_error
            TString DebugInfo() except +raise_py_error
            bool_t HasScope(TStringBuf scopeName) except +raise_py_error
            time_t GetExpirationTime() except +raise_py_error
            const TScopes& GetScopes() except +raise_py_error
            TTvmId GetSrc() except +raise_py_error
            EStatus GetStatus() except +raise_py_error
            TMaybe[TUid] GetIssuerUid() except +raise_py_error

            @staticmethod
            THolder[TCheckedServiceTicket.TImpl] CreateTicketForTests(EStatus, TTvmId, TMaybe[TUid]) except +raise_py_error

    cdef cppclass TServiceContext:
        cppclass TImpl:
            TImpl(TStringBuf secretBase64, int clientId, TStringBuf tvmKeysResponse) except +raise_py_error
            TImpl(int clientId, TStringBuf tvmKeysResponse) except +raise_py_error
            TImpl(TStringBuf secretBase64) except +raise_py_error
            THolder[TCheckedServiceTicket.TImpl] Check(TStringBuf ticketBody) except +raise_py_error
            void ResetKeys(TStringBuf tvmKeysResponse) except +raise_py_error
            TString SignCgiParamsForTvm(TStringBuf ts, TStringBuf dst, TStringBuf scopes) except +raise_py_error const

cdef extern from "library/cpp/tvmauth/src/user_impl.h" namespace "NTvmAuth" nogil:
    cdef cppclass TCheckedUserTicket:
        cppclass TImpl:
            TImpl() except +raise_py_error
            bool_t operator bool() except +raise_py_error
            bool_t HasScope(TStringBuf scopeName) except +raise_py_error
            TString DebugInfo() except +raise_py_error
            TUid GetDefaultUid() except +raise_py_error
            time_t GetExpirationTime() except +raise_py_error
            const TScopes& GetScopes() except +raise_py_error
            EStatus GetStatus() except +raise_py_error
            const TUids& GetUids() except +raise_py_error

            @staticmethod
            THolder[TCheckedUserTicket.TImpl] CreateTicketForTests(EStatus, TUid, TScopes, TUids) except +raise_py_error

    cdef cppclass TUserContext:
        cppclass TImpl:
            TImpl(EBlackboxEnv env, TStringBuf tvmKeysResponse) except +raise_py_error
            THolder[TCheckedUserTicket.TImpl] Check(TStringBuf ticketBody) except +raise_py_error
            void ResetKeys(TStringBuf tvmKeysResponse) except +raise_py_error

cdef extern from "library/cpp/tvmauth/utils.h" namespace "NTvmAuth::NUtils" nogil:
    TStringBuf RemoveTicketSignature(TStringBuf ticketBody) except +raise_py_error

cdef extern from "library/cpp/tvmauth/client/misc/api/settings.h" namespace "NTvmAuth::NTvmApi" nogil:
    cdef cppclass TClientSettings:
        cppclass TDst:
            TDst(TTvmId) except +raise_py_error

        ctypedef THashMap[TString, TDst] TDstMap
        ctypedef TVector[TDst] TDstVector

        TClientSettings() except +raise_py_error
        void CheckValid() except +raise_py_error

        TString DiskCacheDir
        TTvmId SelfTvmId
        TString Secret  # Actual type is NSecretString::TSecretString, but for operator=() it is enough

        TDstVector FetchServiceTicketsForDsts
        TDstMap FetchServiceTicketsForDstsWithAliases
        bool_t CheckServiceTickets
        TMaybe[EBlackboxEnv] CheckUserTicketsWithBbEnv

        TString TvmHost
        int TvmPort


cdef extern from "library/cpp/tvmauth/client/client_status.h" namespace "NTvmAuth" nogil:
    cdef cppclass TClientStatus:
        cppclass ECode "ECode":
            pass
        ECode GetCode()
    TClientStatus.ECode cCsOk "NTvmAuth::TClientStatus::Ok"
    TClientStatus.ECode cCsWarning "NTvmAuth::TClientStatus::Warning"
    TClientStatus.ECode cCsError "NTvmAuth::TClientStatus::Error"


class TvmClientStatus(IntEnum):
    """
    Description:
    https://a.yandex-team.ru/arc/trunk/arcadia/library/python/deprecated/ticket_parser2/README.md#high-level-interface
    """
    Ok = <int>cCsOk
    ExpiringCache = <int>cCsWarning
    InvalidCache = <int>cCsError
    IncompleteListOfServiceTickets = <int>3

cdef extern from "library/cpp/tvmauth/src/utils.h" namespace "NTvmAuth::NInternal" nogil:
    cdef cppclass TCanningKnife:
        @staticmethod
        TCheckedServiceTicket.TImpl* GetS(TCheckedServiceTicket& t)
        @staticmethod
        TCheckedUserTicket.TImpl* GetU(TCheckedUserTicket& t)

cdef extern from "library/python/deprecated/ticket_parser2/src/logger.h" namespace "NTvmAuth" nogil:
    cdef cppclass IPyLogger:
        IPyLogger(cpy_ref.PyObject *obj)
        void Log(int lvl, const TString& msg)

cdef extern from "library/python/deprecated/ticket_parser2/src/utils.h" namespace "NTvmAuth" nogil:
    cdef cppclass TPidCheckedClient:
        TPidCheckedClient(TCustomUpdater*) except +raise_py_error
        TClientStatus GetStatus() except +raise_py_error
        TStringBuf GetStatusString() except +raise_py_error
        TInstant GetUpdateTimeOfPublicKeys() except +raise_py_error
        TInstant GetUpdateTimeOfServiceTickets() except +raise_py_error
        TString GetServiceTicketFor(const TString& dst) except +raise_py_error
        TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) except +raise_py_error
        TCheckedUserTicket CheckUserTicket(TStringBuf ticket) except +raise_py_error
        TCheckedUserTicket CheckUserTicketWithOveridedEnv(TStringBuf ticket, EBlackboxEnv env) except +raise_py_error
        @staticmethod
        TStringBuf StatusToString(TClientStatus.ECode status) except +raise_py_error

    cdef TString GetServiceTicketForId(const TPidCheckedClient&, TTvmId) except +raise_py_error
    cdef TPidCheckedClient* CreateTvmApiClient(TClientSettings& settings, IPyLogger*) except +raise_py_error
    cdef TPidCheckedClient* CreateTvmToolClient(const TTvmToolClientSettings&, IPyLogger*) except +raise_py_error
    cdef TString GetPyVersion() except +raise_py_error
    cdef void StartTvmClientStopping(TPidCheckedClient*)
    cdef bool_t IsTvmClientStopped(TPidCheckedClient*)
    cdef void DestroyTvmClient(TPidCheckedClient*)

    cdef cppclass TCustomUpdater:
        TCustomUpdater(const TClientSettings&, IPyLogger*) except +raise_py_error

    cdef cppclass TTvmToolClientSettings:
        TTvmToolClientSettings(TString) except +raise_py_error
        TTvmToolClientSettings& SetPort(int)
        TTvmToolClientSettings& SetHostname(const TString&) except +raise_py_error
        TTvmToolClientSettings& SetAuthToken(const TString&) except +raise_py_error
        TTvmToolClientSettings& OverrideBlackboxEnv(EBlackboxEnv env) except +raise_py_error

__version__ = GetPyVersion().decode('utf-8')

cdef class ServiceContext:
    """
    WARNING: it is low level API: first of all try use TvmClient. It is not deprecated but don't use it.
    Long lived object for keeping client's credentials for TVM
    """
    cdef TServiceContext.TImpl* baseptr
    def __cinit__(self, int client_id, secret, tvm_keys):
        if tvm_keys is None and secret is None:
            raise ContextException("secret and tvm_keys both can't be None")
        if secret is None:
            self.baseptr = new TServiceContext.TImpl(client_id, <TString>tvm_keys.encode('utf-8'))
        elif tvm_keys is None:
            self.baseptr = new TServiceContext.TImpl(<TString>secret.encode('utf-8'))
        else:
            self.baseptr = new TServiceContext.TImpl(<TString>secret.encode('utf-8'), client_id, <TString>tvm_keys.encode('utf-8'))

    def __dealloc__(self):
        del self.baseptr

    cdef __sign(self, TString timestamp, TString dst, TString scopes):
        return self.baseptr.SignCgiParamsForTvm(timestamp, dst, scopes).decode('utf-8')

    @staticmethod
    cdef __check(TCheckedServiceTicket.TImpl* ticket):
        tick = ServiceTicket()
        tick.baseptr = ticket
        raw_status = tick.baseptr.GetStatus()
        status = Status(<int>raw_status)
        if status != Status.Ok:
            raise TicketParsingException(StatusToString(raw_status).decode('utf-8'), status, tick.debug_info())
        return tick

    def check(self, ticket_body):
        return ServiceContext.__check(self.baseptr.Check(<TString>ticket_body.encode('utf-8')).Release())

    def reset_keys(self, tvm_keys):
        self.baseptr.ResetKeys(<TString>tvm_keys.encode('utf-8'))

    def sign(self, timestamp, dst, scopes=None):
        if isinstance(timestamp, int):
            timestamp = str(timestamp)
        if isinstance(dst, list):
            dst = ','.join(map(lambda x: x if isinstance(x, str) else str(x), dst))
        elif isinstance(dst, int):
            dst = str(dst)
        if isinstance(scopes, list):
            scopes = ','.join(map(lambda x: x if isinstance(x, str) else str(x), scopes))
        elif scopes is None:
            scopes = ''
        return self.__sign(timestamp.encode('utf-8'), dst.encode('utf-8'), scopes.encode('utf-8'))

cdef class ServiceTicket:
    cdef TCheckedServiceTicket.TImpl* baseptr
    def __dealloc__(self):
        del self.baseptr

    def __nonzero__(self):
        return <bool_t>(dereference(self.baseptr))

    def __str__(self):
        return self.debug_info()

    def __repr__(self):
        return str(self)

    def debug_info(self):
        """
        :return: Human readable data for debug purposes
        """
        return self.baseptr.DebugInfo().decode('utf8')

    def has_scope(self, scope_name):
        return self.baseptr.HasScope(<TString>scope_name.encode('utf-8'))

    @staticmethod
    def remove_signature(ticket_body):
        """
        :param ticket_body: Full ticket body
        :return:            Safe for logging part of ticket - it can be parsed later with `tvmknife parse_ticket -t ...`
        """
        return RemoveTicketSignature(<TString>ticket_body.encode('utf-8')).decode('utf-8')

    @property
    def scopes(self):
        rlist = []
        scopes = self.baseptr.GetScopes()
        for i in range(scopes.size()):
            rlist.append(scopes[i].decode('utf-8'))
        return rlist

    @property
    def src(self):
        """
        You should check SrcID by yourself with your ACL.

        :return: ID of request source service
        """
        return self.baseptr.GetSrc()

    @property
    def issuer_uid(self):
        """
        IssuerUID is UID of developer who is debuging something, so he(she) issued ServiceTicket with his(her) ssh-sign:
	    it is grant_type=sshkey in tvm-api
	    https://wiki.yandex-team.ru/passport/tvm2/debug/#sxoditvapizakrytoeserviceticketami.

        :return: UID or `None`
        """
        u = self.baseptr.GetIssuerUid()

        if u.Defined():
            return u.GetRef()


def create_service_ticket_for_unittest(status, int src, issuer_uid=None):
    cdef TMaybe[TUid] uid
    if issuer_uid is not None:
        uid = <int>issuer_uid
    return ServiceContext.__check(TCheckedServiceTicket.TImpl.CreateTicketForTests(
        <EStatus><int>status, src, uid).Release())


cdef class UserContext:
    """
    WARNING: it is low level API: first of all try use TvmClient. It is not deprecated but don't use it.
    Long lived object for keeping client's credentials for TVM
    """
    cdef TUserContext.TImpl* baseptr
    def __cinit__(self, int env, tvm_keys):
        self.baseptr = new TUserContext.TImpl(<EBlackboxEnv>env, <TString>tvm_keys.encode('utf-8'))

    def __dealloc__(self):
        del self.baseptr

    @staticmethod
    cdef __check(TCheckedUserTicket.TImpl* ticket):
        tick = UserTicket()
        tick.baseptr = ticket
        raw_status = tick.baseptr.GetStatus()
        status = Status(<int>raw_status)
        if status != Status.Ok:
            raise TicketParsingException(StatusToString(raw_status).decode('utf-8'), status, tick.debug_info())
        return tick

    def check(self, ticket_body):
        return UserContext.__check(self.baseptr.Check(<TString>ticket_body.encode('utf-8')).Release())

    def reset_keys(self, tvm_keys):
        self.baseptr.ResetKeys(<TString>tvm_keys.encode('utf-8'))

cdef class UserTicket:
    """
    UserTicket contains only valid users.
    Details: https://wiki.yandex-team.ru/passport/tvm2/user-ticket/#chtoestvusertickete
    """
    cdef TCheckedUserTicket.TImpl* baseptr
    def __dealloc__(self):
        del self.baseptr

    def __str__(self):
        return self.debug_info()

    def __repr__(self):
        return str(self)

    def debug_info(self):
        """
        :return: Human readable data for debug purposes
        """
        return self.baseptr.DebugInfo().decode('utf8')

    @property
    def default_uid(self):
        """
        Default UID maybe 0

        :return: default user
        """
        return self.baseptr.GetDefaultUid()

    def has_scope(self, scope_name):
        return self.baseptr.HasScope(<TString>scope_name.encode('utf-8'))

    @staticmethod
    def remove_signature(ticket_body):
        """
        :param ticket_body: Full ticket body
        :return:            Safe for logging part of ticket - it can be parsed later with `tvmknife parse_ticket -t ...`
        """
        return RemoveTicketSignature(<TString>ticket_body.encode('utf-8')).decode('utf-8')

    @property
    def scopes(self):
        """
        Scopes is array of scopes inherited from credential - never empty

        :return: Array of scopes
        """
        rlist = []
        scopes = self.baseptr.GetScopes()
        for i in range(scopes.size()):
            rlist.append(scopes[i].decode('utf-8'))
        return rlist

    @property
    def uids(self):
        """
        UIDs is array of valid users - never empty

        :return: Array of valid users
        """
        rlist = []
        uids = self.baseptr.GetUids()
        for i in range(uids.size()):
            rlist.append(uids[i])
        return rlist

    def __nonzero__(self):
        return <bool_t>(dereference(self.baseptr))


def create_user_ticket_for_unittest(status, int default_uid, scopes=[], uids=[]):
    cdef TScopes sc
    cdef TVector[TString] sc_tmp
    cdef TUids ui

    for v in scopes:
        sc_tmp.push_back(v.encode('utf-8'))
        sc.push_back(sc_tmp.back())
    for v in uids:
        ui.push_back(<int>v)

    return UserContext.__check(TCheckedUserTicket.TImpl.CreateTicketForTests(
        <EStatus><int>status, default_uid, sc, ui).Release())


cdef class TvmApiClientSettings:
    """
    Settings for TVM client. Uses https://tvm-api.yandex.net to get state.
    """
    cdef TClientSettings* baseptr

    def __init__(self,
                 self_client_id=None,
                 enable_service_ticket_checking=False,
                 enable_user_ticket_checking=None,
                 self_secret=None,
                 dsts=None,
                 ):
        """
        Examples:
        - Checking of ServiceTickets:
            TvmApiClientSettings(self_client_id=100500, enable_service_ticket_checking=True)
        - Checking of UserTickets:
            TvmApiClientSettings(enable_user_ticket_checking=BlackboxEnv.Test)
        - Fetching of ServiceTickets (with aliases):
            # init
            s = TvmApiClientSettings(
                  self_client_id=100500,
                  self_secret='my_secret',
                  dsts={'my backend': int(config.get_back_client_id())},
            )
            ...
            # per request
            service_ticket_for_backend = c.get_service_ticket_for('my_backend')

            # Key in dict is internal ALIAS of destination in your code.
            # It allowes not to bring destination's client_id to each calling point.
        - Fetching of ServiceTickets (with client_id):
            # init
            s = TvmApiClientSettings(
                  self_client_id=100500,
                  self_secret='my_secret',
                  dsts=[42],
            )
            ...
            # per request
            service_ticket_for_backend = c.get_service_ticket_for(42)

        :param self_client_id:                 int - TVM-client_id of your service
        :param enable_service_ticket_checking: boolean - flag for SeviceTicket checking
        :param enable_user_ticket_checking:    enum EBlackboxEnv - blackbox enviroment enables UserTicket checking with env
        :param self_secret:                    string - TVM-secret of your service
        :param dsts:                           dict (string -> int) - map of alias to client_id of your destination
                                               or list (int) - client_id of your destination

        :raises `~BrokenTvmClientSettings`:  Raised in case of settings validation fails.
        """
        pass

    def __cinit__(self,
                  self_client_id=None,
                  enable_service_ticket_checking=False,
                  enable_user_ticket_checking=None,
                  self_secret=None,
                  dsts=None,
                  ):
        self.baseptr = new TClientSettings()

        if self_client_id is not None:
            self.baseptr.SelfTvmId = <int>self_client_id

        if enable_service_ticket_checking:
            self.baseptr.CheckServiceTickets = True

        if enable_user_ticket_checking is not None:  # check for None because enum has valid value == 0
            self.baseptr.CheckUserTicketsWithBbEnv = <EBlackboxEnv><int>enable_user_ticket_checking

        if self_secret:
            self.baseptr.Secret = <TString>self_secret.encode('utf-8')

        if isinstance(dsts, dict):
            for k, v in dsts.items():
                self.baseptr.FetchServiceTicketsForDstsWithAliases.insert(pair[TString, TClientSettings.TDst](k.encode('utf-8'), TClientSettings.TDst(<int>v)))
        elif isinstance(dsts, list):
            for v in dsts:
                self.baseptr.FetchServiceTicketsForDsts.push_back(TClientSettings.TDst(<int>v))
        elif dsts is not None:
            raise TvmException("dsts must be dict or list")

        self.baseptr.CheckValid()

    def __dealloc__(self):
        del self.baseptr

    def set_disk_cache_dir(self, dir):
        """
        Set path to directory for disk cache
        Requires read/write permissions. Checks permissions
        WARNING: The same directory can be used only:
                   - for TVM clients with the same settings
                 OR
                   - for new client replacing previous - with another config.
                 System user must be the same for processes with these clients inside.
                 Implementation doesn't provide other scenarios.

        :param dir: directory should exist
        """
        self.baseptr.DiskCacheDir = <TString>dir.encode('utf-8')

    cdef TString __host
    cdef int __port

    def __set_localhost(self, port):
        self.__host = TString('localhost')
        self.__port = <int>port
        self.baseptr.TvmHost = self.__host
        self.baseptr.TvmPort = self.__port


cdef class TvmToolClientSettings:
    """
    Uses local http-interface to get state: http://localhost/tvm/.
    This interface can be provided with tvmtool (local daemon) or Qloud/YP (local http api in container).
    See more: https://wiki.yandex-team.ru/passport/tvm2/qloud/.
    """
    cdef TTvmToolClientSettings* baseptr

    def __init__(self,
                 self_alias,
                 auth_token=None,
                 port=None,
                 hostname="localhost",
                 override_bb_env=None,
                 ):
        """
        Examples:
        - Ctor for Qloud:
            TvmToolClientSettings("me") # 'me' was specified as alias for your tvm client_id in Qloud interface
        - Ctor for local tvmtool:
            TvmToolClientSettings("me", auth_token="AAAAAAAAAAAAAAAAAAAAAA", port=18080)
        - Ctor for remote tvmtool - in dev-environment (if you need this):
            TvmToolClientSettings("me", auth_token="AAAAAAAAAAAAAAAAAAAAAA", port=18080, hostname="front.dev.yandex.net")
        - Get ticket from client:
            c = TvmClient(TvmToolClientSettings("me"))  # 'me' was specified as alias for your tvm client in Qloud interface
            t = c.get_service_ticket_for("push-client") # 'push-client' was specified as alias for dst in Qloud interface
            t = c.get_service_ticket_for(100500)        # 100500 was specified as dst in Qloud interface
        - Check user ticket for another bb_env:
            TvmToolClientSettings("me", override_bb_env=BlackboxEnv.ProdYateam)  # BlackboxEnv.Prod was specified for tvmtool

        :param self_alias:      string - alias for your TVM-client_id - needs to be specified in settings of tvmtool/Qloud/YP
        :param auth_token:      string - default value == env['TVMTOOL_LOCAL_AUTHTOKEN'] (provided with Yandex.Deploy) or env['QLOUD_TVM_TOKEN'] (provided with Qloud)
        :param port:            int - autodetected for Qloud/YP. TCP port for HTTP-interface provided with tvmtool/Qloud/YP
        :param hostname:        string - hostname for tvmtool
        :param override_bb_env: enum EBlackboxEnv - blackbox enviroment overrides env from tvmtool. Allowed only:
                                - Prod/ProdYateam -> Prod/ProdYateam
                                - Test/TestYateam -> Test/TestYateam
                                - Stress -> Stress
                                You can contact tvm-dev@yandex-team.ru if limitations are too strict
        """
        pass

    def __cinit__(self,
                  self_alias,
                  auth_token=None,
                  port=None,
                  hostname="localhost",
                  override_bb_env=None,
                  ):
        self.baseptr = new TTvmToolClientSettings(<TString>self_alias.encode('utf-8'))

        if auth_token is not None:
            self.baseptr.SetAuthToken(<TString>auth_token.encode('utf-8'))

        if port is not None:
            self.baseptr.SetPort(<int>port)

        if hostname is not None:
            self.baseptr.SetHostname(<TString>hostname.encode('utf-8'))

        if override_bb_env is not None:
            self.baseptr.OverrideBlackboxEnv(<EBlackboxEnv><int>override_bb_env)

    def __dealloc__(self):
        del self.baseptr


cdef public api void cy_call_func(object self, char* method, int lvl, const char* data, size_t length) with gil:
    cdef object message = data[:length]

    try:
        func = getattr(self, method.decode("utf-8"));
    except AttributeError as e:
        return

    func(lvl, message.rstrip())


cdef class TvmClient:
    """
    Long lived thread-safe object for interacting with TVM.
    Each client starts system thread. !!SO DO NOT FORK YOUR PROCESS AFTER CREATING TvmClient!!
    In 99% cases TvmClient shoud be created at service startup and live for the whole process lifetime.
    If your case in this 1% and you need to RESTART client, you should to use method 'stop()' for old client.

    You can get logs from 'TVM':
        log = logging.getLogger('TVM')
    """
    cdef TPidCheckedClient* baseptr

    __logger = logging.getLogger('TVM')
    __loghandles = {
        0: __logger.error,
        1: __logger.error,
        2: __logger.error,
        3: __logger.error,
        4: __logger.warning,
        5: __logger.info,
        6: __logger.info,
        7: __logger.debug,
    }

    def __init__(self, settings):
        """
        :param settings: TvmApiClientSettings or TvmToolClientSettings - settings for client

        :raises `NonRetriableException`: Raised in case of settings validation fails.
        :raises `RetriableException`:    Raised if network unavailable.
        """
        pass

    def __cinit__(self, settings):
        if isinstance(settings, TvmToolClientSettings):
            self.baseptr = CreateTvmToolClient(dereference((<TvmToolClientSettings>settings).baseptr),\
                                            new IPyLogger(<cpy_ref.PyObject*>self))
        elif isinstance(settings, TvmApiClientSettings):
            if (<TvmApiClientSettings>settings).__host.empty():
                self.baseptr = CreateTvmApiClient(dereference((<TvmApiClientSettings>settings).baseptr),\
                                                new IPyLogger(<cpy_ref.PyObject*>self))
            else:
                self.baseptr = new TPidCheckedClient(new TCustomUpdater( \
                    dereference((<TvmApiClientSettings>settings).baseptr), \
                    new IPyLogger(<cpy_ref.PyObject*>self)))
        else:
            raise BrokenTvmClientSettings("'settings' must be instance of TvmApiClientSettings or TvmToolClientSettings")

    def stop(self):
        """
        First call will delete object. Next calls will be no-op.
        """
        # stop() (and delete inside of it) probably will be called from main thread.
        # There is TThread::Join() inside which waites worker thread.
        # Worker thread can reach state when it is waiting for main thread: it needs to acquire lock in logging,
        #  but it can be released only by main thread.
        # We can't delete TvmClient with one step because of deadlock (PASSP-21494).
        # It is C++ function which is not interruptible:
        #  so we are interrupting C++ function by handes to allow main Python thread to release lock
        StartTvmClientStopping(self.baseptr)
        while not IsTvmClientStopped(self.baseptr):
            time.sleep(0.1)
        DestroyTvmClient(self.baseptr)
        self.baseptr = NULL

    def __dealloc__(self):
        self.stop()

    def __log(self, int lvl, msg):
        self.__loghandles[lvl](msg.decode("utf-8"))

    @property
    def status(self):
        """
        :return: Current status of client - :class:`~TvmClientStatus`
        """
        self.__check()
        return TvmClientStatus(<int>self.baseptr.GetStatus().GetCode())

    @staticmethod
    def status_to_string(status):
        """
        :return: Status of client as string
        """
        return TPidCheckedClient.StatusToString(<TClientStatus.ECode><int>status).decode('utf-8')

#    @property
#    def last_update_time_of_public_keys(self):
#        self.__check()
#        return datetime.datetime.fromtimestamp(self.baseptr.GetUpdateTimeOfPublicKeys().Seconds())

#    @property
#    def last_update_time_of_service_tickets(self):
#        self.__check()
#        return datetime.datetime.fromtimestamp(self.baseptr.GetUpdateTimeOfServiceTickets().Seconds())

    def get_service_ticket_for(self, alias=None, client_id=None):
        """
        Fetching must be enabled in TvmApiClientSettings

        :param alias: string - see docstring for TvmApiClientSettings.__init__
        :param client_id: int - any destination you specified in TvmApiClientSettings

        :raises `BrokenTvmClientSettings`: Raised in case of unconfigured using.

        :returns: string - ServiceTicket
       """
        self.__check()
        if alias is not None:
            return self.baseptr.GetServiceTicketFor(<TString>alias.encode('utf-8')).decode('utf-8')
        if client_id is not None:
            return GetServiceTicketForId(dereference(self.baseptr), int(client_id)).decode('utf-8')
        raise TvmException("One of args is required: 'alias' or 'client_id'")

    def check_service_ticket(self, ticket):
        """
        :param ticket: string - ticket body

        :raises `BrokenTvmClientSettings`: Raised in case of unconfigured using.
        :raises `TicketParsingException`:  Raised in case of invalid ticket.

        :return: Valid ticket structure
        """
        self.__check()
        return ServiceContext.__check(TCanningKnife.GetS(self.baseptr.CheckServiceTicket(<TString>ticket.encode('utf-8'))))

    def check_user_ticket(self, ticket, overrided_bb_env=None):
        """
        :param ticket: string - ticket body
        :param overrided_bb_env: enum EBlackboxEnv

        :raises `BrokenTvmClientSettings`: Raised in case of unconfigured using.
        :raises `TicketParsingException`:  Raised in case of invalid ticket.

        :return: Valid ticket structure
        """
        self.__check()
        if overrided_bb_env is None:
            return UserContext.__check(TCanningKnife.GetU(
                self.baseptr.CheckUserTicket(<TString>ticket.encode('utf-8'))))
        else:
            return UserContext.__check(TCanningKnife.GetU(
                self.baseptr.CheckUserTicketWithOveridedEnv(<TString>ticket.encode('utf-8'), <EBlackboxEnv><int>overrided_bb_env)))

    def __check(self):
        if NULL == self.baseptr:
            raise NonRetriableException("TvmClient is already stopped")

PyEval_InitThreads()
