# coding: utf-8
import datetime
import logging
import six
import time
from cpython cimport PyObject
from libcpp cimport bool as bool_t
from libcpp.map cimport map as cmap
from libcpp.memory cimport shared_ptr as cshared_ptr
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libc.stdint cimport uint32_t, uint64_t
from libc.time cimport time_t
from cython.operator cimport dereference
cimport cpython.ref as cpy_ref
import urllib3.util

from enum import IntEnum

from util.datetime.base cimport TInstant
from util.generic.hash cimport THashMap
from util.generic.ptr cimport THolder
from util.generic.string cimport TString, TStringBuf
from util.generic.vector cimport TVector
from util.generic.maybe cimport TMaybe


__doc__ = 'WARNING: This is internal part of library - so it is not the public API of library. It could be changed without changing of major version.'


cdef extern from "Python.h":
    """
    #if PY_VERSION_HEX < 0x03090000
      #define _TA_InitThreads()  PyEval_InitThreads()
    #else
      #define _TA_InitThreads()
    #endif
    """
    void TA_InitThreads "_TA_InitThreads"()

class __TvmException(Exception):
    pass

class __ContextException(__TvmException):
    pass

class __EmptyTvmKeysException(__ContextException):
    pass

class __MalformedTvmKeysException(__ContextException):
    pass

class __MalformedTvmSecretException(__ContextException):
    pass

class NotAllowedException(__TvmException):
    pass

class __ClientException(__TvmException):
    pass

class __RetriableException(__ClientException):
    pass

class __NonRetriableException(__ClientException):
    pass

class __BrokenTvmClientSettings(__NonRetriableException):
    pass

class __MissingServiceTicket(__NonRetriableException):
    pass

class __PermissionDenied(__NonRetriableException):
    pass

@six.python_2_unicode_compatible
class __TicketParsingException(__TvmException):
    def __init__(self, message, status, debug_info):
        self.message = message
        self.status = status
        self.debug_info = debug_info

    def __str__(self):
        return u'%s: %s' % (self.message, self.debug_info)


cdef public PyObject* TA_pyEmptyTvmKeysException = <PyObject*>__EmptyTvmKeysException
cdef public PyObject* TA_pyMalformedTvmKeysException = <PyObject*>__MalformedTvmKeysException
cdef public PyObject* TA_pyMalformedTvmSecretException = <PyObject*>__MalformedTvmSecretException
cdef public PyObject* TA_pyNotAllowedException = <PyObject*>NotAllowedException
cdef public PyObject* TA_pyClientException = <PyObject*>__ClientException
cdef public PyObject* TA_pyBrokenTvmClientSettings = <PyObject*>__BrokenTvmClientSettings
cdef public PyObject* TA_pyRetriableException = <PyObject*>__RetriableException
cdef public PyObject* TA_pyNonRetriableException = <PyObject*>__NonRetriableException
cdef public PyObject* TA_pyMissingServiceTicket = <PyObject*>__MissingServiceTicket
cdef public PyObject* TA_pyPermissionDenied = <PyObject*>__PermissionDenied


cdef extern from "library/cpp/containers/stack_vector/stack_vec.h" nogil:
    cdef cppclass TSmallVec[T](TVector):
        pass

cdef extern from "library/python/tvmauth/src/exception.h":
    cdef void TA_raise_py_error()

cdef extern from "library/cpp/tvmauth/ticket_status.h" namespace "NTvmAuth" nogil:
    cdef cppclass EStatus "NTvmAuth::ETicketStatus":
        pass
    TStringBuf StatusToString(EStatus status)

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
    cdef EStatus cNoRoles "NTvmAuth::ETicketStatus::NoRoles"

class __TicketStatus(IntEnum):
    """
    __TicketStatus mean result of ticket check
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
    NoRoles = <int>cNoRoles

cdef extern from "library/cpp/tvmauth/checked_user_ticket.h" namespace "NTvmAuth" nogil:
    cdef cppclass EBlackboxEnv "NTvmAuth::EBlackboxEnv":
        pass


cdef extern from "library/cpp/tvmauth/type.h" namespace "NTvmAuth" nogil:
    ctypedef uint32_t TTvmId
    ctypedef uint64_t TUid

    cdef cppclass TScopes(TSmallVec[TStringBuf]):
        pass

    cdef cppclass TUids(TSmallVec[TUid]):
        pass

cdef extern from "library/cpp/tvmauth/utils.h" namespace "NTvmAuth::NUtils" nogil:
    TStringBuf RemoveTicketSignature(TStringBuf ticketBody) except +TA_raise_py_error

cdef extern from "library/cpp/tvmauth/src/service_impl.h" namespace "NTvmAuth" nogil:
    cdef cppclass TCheckedServiceTicket:
        bool_t operator bool() except +TA_raise_py_error
        TString DebugInfo() except +TA_raise_py_error
        TTvmId GetSrc() except +TA_raise_py_error
        EStatus GetStatus() except +TA_raise_py_error
        TMaybe[TUid] GetIssuerUid() except +TA_raise_py_error

    cdef cppclass TServiceContext:
        TServiceContext(TStringBuf secretBase64, int tvmId, TStringBuf tvmKeysResponse) except +TA_raise_py_error
        TCheckedServiceTicket Check(TStringBuf ticketBody) except +TA_raise_py_error
        TString SignCgiParamsForTvm(TStringBuf ts, TStringBuf dst, TStringBuf scopes) except +TA_raise_py_error const

cdef extern from "library/cpp/tvmauth/unittest.h" namespace "NTvmAuth::NUnittest" nogil:
    TCheckedUserTicket CreateUserTicket(EStatus, TUid, TScopes, TUids, EBlackboxEnv) except +TA_raise_py_error
    TCheckedServiceTicket CreateServiceTicket(EStatus, TTvmId, TMaybe[TUid]) except +TA_raise_py_error

cdef extern from "library/cpp/tvmauth/src/user_impl.h" namespace "NTvmAuth" nogil:
    cdef cppclass TCheckedUserTicket:
        bool_t operator bool() except +TA_raise_py_error
        bool_t HasScope(TStringBuf scopeName) except +TA_raise_py_error
        TString DebugInfo() except +TA_raise_py_error
        TUid GetDefaultUid() except +TA_raise_py_error
        time_t GetExpirationTime() except +TA_raise_py_error
        const TScopes& GetScopes() except +TA_raise_py_error
        EStatus GetStatus() except +TA_raise_py_error
        const TUids& GetUids() except +TA_raise_py_error

    cdef cppclass TUserContext:
        TUserContext(EBlackboxEnv env, TStringBuf tvmKeysResponse) except +TA_raise_py_error
        TCheckedUserTicket Check(TStringBuf ticketBody) except +TA_raise_py_error


cdef extern from "library/cpp/tvmauth/client/misc/api/settings.h" namespace "NTvmAuth::NTvmApi" nogil:
    cdef cppclass TClientSettings:
        cppclass TDst:
            TDst(TTvmId) except +TA_raise_py_error

        ctypedef THashMap[TString, TDst] TDstMap
        ctypedef TVector[TDst] TDstVector

        TClientSettings() except +TA_raise_py_error
        void CheckValid() except +TA_raise_py_error

        TString DiskCacheDir
        TTvmId SelfTvmId
        TString Secret  # Actual type is NSecretString::TSecretString, but for operator=() it is enough

        TDstVector FetchServiceTicketsForDsts
        TDstMap FetchServiceTicketsForDstsWithAliases
        bool_t CheckServiceTickets
        TMaybe[EBlackboxEnv] CheckUserTicketsWithBbEnv

        TString FetchRolesForIdmSystemSlug
        bool_t ShouldCheckSrc
        bool_t ShouldCheckDefaultUid

        TString TvmHost
        int TvmPort
        TString TiroleHost
        int TirolePort
        TTvmId TiroleTvmId


cdef extern from "library/cpp/tvmauth/client/client_status.h" namespace "NTvmAuth" nogil:
    cdef cppclass TClientStatus:
        cppclass ECode "ECode":
            pass
        ECode GetCode()
        const TString& GetLastError()

cdef extern from "library/python/tvmauth/src/utils.h" namespace "NTvmAuthPy" nogil:
    cdef cppclass TOptUid:
        TOptUid()
        TOptUid(TUid)


cdef extern from "library/cpp/tvmauth/client/misc/roles/roles.h" namespace "NTvmAuth::NRoles" nogil:
    ctypedef cmap[TString, TString] TEntity

    cdef cppclass TEntities:
        bool_t Contains(const TEntity&) except +TA_raise_py_error
        vector[cshared_ptr[TEntity]]& GetEntitiesWithAttrs(const TEntity&) except +TA_raise_py_error

    ctypedef THashMap[TString, cshared_ptr[TEntities]] TEntitiesByRoles

    cdef cppclass TConsumerRoles:
        bool_t HasRole(TStringBuf) except +TA_raise_py_error
        TEntitiesByRoles& GetRoles() except +TA_raise_py_error
        cshared_ptr[TEntities] GetEntitiesForRole(const TStringBuf) except +TA_raise_py_error
        bool_t CheckRoleForExactEntity(TStringBuf, const TEntity&) except +TA_raise_py_error

    cdef cppclass TRoles:
        cppclass TMeta:
            TString Revision
            TInstant BornTime
            TInstant Applied

        const TMeta& GetMeta() except +TA_raise_py_error
        const TString& GetRaw() except +TA_raise_py_error
        cshared_ptr[TConsumerRoles] GetRolesForService(const TCheckedServiceTicket&) except +TA_raise_py_error
        cshared_ptr[TConsumerRoles] GetRolesForUser(const TCheckedUserTicket&, TOptUid) except +TA_raise_py_error
        bool_t CheckServiceRole(const TCheckedServiceTicket&, const TStringBuf) except +TA_raise_py_error
        bool_t CheckUserRole(const TCheckedUserTicket&, const TStringBuf, TOptUid) except +TA_raise_py_error
        bool_t CheckServiceRoleForExactEntity(const TCheckedServiceTicket&, const TStringBuf, const TEntity&) except +TA_raise_py_error
        bool_t CheckUserRoleForExactEntity(const TCheckedUserTicket&, const TStringBuf, const TEntity&, TOptUid) except +TA_raise_py_error


cdef extern from "library/python/tvmauth/src/logger.h" namespace "NTvmAuthPy" nogil:
    cdef cppclass TPyLogger:
        TPyLogger(cpy_ref.PyObject *obj)
        cppclass TPyLoggerPtr:
            TPyLoggerPtr()
        @staticmethod
        TPyLoggerPtr Create() except +TA_raise_py_error
        @staticmethod
        TVector[pair[int, TString]] FetchMessages(TPyLoggerPtr ptr) except +TA_raise_py_error


cdef extern from "library/python/tvmauth/src/utils.h" namespace "NTvmAuthPy" nogil:
    cdef cppclass TPidCheckedClient:
        TClientStatus GetStatus() except +TA_raise_py_error
        TStringBuf GetStatusString() except +TA_raise_py_error
        TInstant GetUpdateTimeOfPublicKeys() except +TA_raise_py_error
        TInstant GetUpdateTimeOfServiceTickets() except +TA_raise_py_error
        TString GetServiceTicketFor(const TString& dst) except +TA_raise_py_error
        TCheckedServiceTicket CheckServiceTicket(TStringBuf ticket) except +TA_raise_py_error
        TCheckedUserTicket CheckUserTicket(TStringBuf ticket) except +TA_raise_py_error
        TCheckedUserTicket CheckUserTicketWithOveridedEnv(TStringBuf ticket, EBlackboxEnv env) except +TA_raise_py_error
        cshared_ptr[TRoles] GetRoles() except +TA_raise_py_error

    cdef T&& Move[T](T&)
    cdef THolder[T] ToHeap[T](T&)

    @staticmethod
    cdef THolder[TServiceContext] CheckingFactory(int tvmId, TStringBuf tvmKeysResponse) except +TA_raise_py_error
    @staticmethod
    cdef THolder[TServiceContext] SigningFactory(TStringBuf secretBase64) except +TA_raise_py_error

    cdef TString GetServiceTicketForId(const TPidCheckedClient&, TTvmId) except +TA_raise_py_error
    cdef TPidCheckedClient* CreateTvmApiClient(TClientSettings& settings, TPyLogger.TPyLoggerPtr) except +TA_raise_py_error
    cdef TPidCheckedClient* CreateTvmToolClient(const TTvmToolClientSettings&, TPyLogger.TPyLoggerPtr) except +TA_raise_py_error
    cdef TString GetPyVersion() except +TA_raise_py_error

    cdef cppclass TTvmToolClientSettings:
        TTvmToolClientSettings(TString) except +TA_raise_py_error
        TTvmToolClientSettings& SetPort(int)
        TTvmToolClientSettings& SetHostname(const TString&) except +TA_raise_py_error
        TTvmToolClientSettings& SetAuthToken(const TString&) except +TA_raise_py_error
        TTvmToolClientSettings& OverrideBlackboxEnv(EBlackboxEnv env) except +TA_raise_py_error

        bool_t ShouldCheckSrc
        bool_t ShouldCheckDefaultUid

__version__ = GetPyVersion().decode('utf-8')

cdef class __ServiceContext:
    cdef THolder[TServiceContext] baseptr
    def __init__(self, tvm_id, secret, tvm_keys):
        pass

    def __cinit__(self, tvm_id, secret, tvm_keys):
        if tvm_keys is None and secret is None:
            raise __ContextException("secret and tvm_keys both can't be None")
        if secret is None:
            self.baseptr = Move(CheckingFactory(<int>tvm_id, <TString>tvm_keys.encode('utf-8')))
        elif tvm_keys is None:
            self.baseptr = Move(SigningFactory(<TString>secret.encode('utf-8')))
        else:
            self.baseptr.Reset(new TServiceContext(<TString>secret.encode('utf-8'), <int>tvm_id, <TString>tvm_keys.encode('utf-8')))

    cdef __sign(self, TString timestamp, TString dst, TString scopes):
        return self.baseptr.Get().SignCgiParamsForTvm(timestamp, dst, scopes).decode('utf-8')

    @staticmethod
    cdef __check(TCheckedServiceTicket ticket):
        tick = __CheckedServiceTicket()
        tick.baseptr = ToHeap(ticket)
        raw_status = tick.baseptr.Get().GetStatus()
        status = __TicketStatus(<int>raw_status)
        if status != __TicketStatus.Ok:
            raise __TicketParsingException(StatusToString(raw_status).decode('utf-8'), status, tick.debug_info())
        return tick

    def check(self, ticket_body):
        return __ServiceContext.__check(Move(self.baseptr.Get().Check(<TString>ticket_body.encode('utf-8'))))

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

cdef class __CheckedServiceTicket:
    cdef THolder[TCheckedServiceTicket] baseptr

    def __nonzero__(self):
        return <bool_t>(self.baseptr.Get())

    def debug_info(self):
        return self.baseptr.Get().DebugInfo().decode('utf8')

    @property
    def src(self):
        return self.baseptr.Get().GetSrc()

    @property
    def issuer_uid(self):
        u = self.baseptr.Get().GetIssuerUid()

        if u.Defined():
            return u.GetRef()


def __create_service_ticket_for_unittest(status, int src, issuer_uid=None):
    cdef TMaybe[TUid] uid
    if issuer_uid is not None:
        uid = <int>issuer_uid
    return __ServiceContext.__check(Move(CreateServiceTicket(
        <EStatus><int>status, src, uid)))


cdef class __UserContext:
    cdef THolder[TUserContext] baseptr
    def __init__(self, env, tvm_keys):
        pass

    def __cinit__(self, int env, tvm_keys):
        self.baseptr.Reset(new TUserContext(<EBlackboxEnv>env, <TString>tvm_keys.encode('utf-8')))

    @staticmethod
    cdef __check(TCheckedUserTicket ticket):
        tick = __CheckedUserTicket()
        tick.baseptr = ToHeap(ticket)
        raw_status = tick.baseptr.Get().GetStatus()
        status = __TicketStatus(<int>raw_status)
        if status != __TicketStatus.Ok:
            raise __TicketParsingException(StatusToString(raw_status).decode('utf-8'), status, tick.debug_info())
        return tick

    def check(self, ticket_body):
        return __UserContext.__check(Move(self.baseptr.Get().Check(<TString>ticket_body.encode('utf-8'))))

cdef class __CheckedUserTicket:
    cdef THolder[TCheckedUserTicket] baseptr

    def debug_info(self):
        return self.baseptr.Get().DebugInfo().decode('utf8')

    @property
    def default_uid(self):
        return self.baseptr.Get().GetDefaultUid()

    def has_scope(self, scope_name):
        return self.baseptr.Get().HasScope(<TString>scope_name.encode('utf-8'))

    @property
    def scopes(self):
        rlist = []
        scopes = self.baseptr.Get().GetScopes()
        for i in range(scopes.size()):
            rlist.append(scopes[i].decode('utf-8'))
        return rlist

    @property
    def uids(self):
        rlist = []
        uids = self.baseptr.Get().GetUids()
        for i in range(uids.size()):
            rlist.append(uids[i])
        return rlist

    def __nonzero__(self):
        return <bool_t>(self.baseptr.Get())


def __create_user_ticket_for_unittest(status, int default_uid, scopes, uids, env):
    cdef TScopes sc
    cdef TVector[TString] sc_tmp
    cdef TUids ui

    for v in scopes:
        sc_tmp.push_back(v.encode('utf-8'))
        sc.push_back(sc_tmp.back())
    for v in uids:
        ui.push_back(<int>v)

    return __UserContext.__check(Move(CreateUserTicket(
        <EStatus><int>status, default_uid, sc, ui, <EBlackboxEnv><int>env)))


def __remove_ticket_signature(ticket_body):
    return RemoveTicketSignature(<TString>ticket_body.encode('utf-8')).decode('utf-8')


cdef class __TvmApiClientSettings:
    cdef TClientSettings* baseptr

    def __init__(self, *args, **kwargs):
        pass

    def __cinit__(self,
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
        self.baseptr = new TClientSettings()

        if self_tvm_id:
            self.baseptr.SelfTvmId = <int>self_tvm_id

        if enable_service_ticket_checking:
            self.baseptr.CheckServiceTickets = True

        if enable_user_ticket_checking is not None:  # check for None because enum has valid value == 0
            self.baseptr.CheckUserTicketsWithBbEnv = <EBlackboxEnv><int>enable_user_ticket_checking

        if not check_src_by_default:
            self.baseptr.ShouldCheckSrc = False

        if not check_default_uid_by_default:
            self.baseptr.ShouldCheckDefaultUid = False

        if self_secret:
            self.baseptr.Secret = <TString>self_secret.encode('utf-8')

            if isinstance(dsts, dict):
                for k, v in dsts.items():
                    self.baseptr.FetchServiceTicketsForDstsWithAliases.insert(pair[TString, TClientSettings.TDst](k.encode('utf-8'), TClientSettings.TDst(<int>v)))
            elif isinstance(dsts, list):
                for v in dsts:
                    self.baseptr.FetchServiceTicketsForDsts.push_back(TClientSettings.TDst(<int>v))
            elif dsts is not None:
                raise __TvmException("dsts must be dict or list or None")

        if disk_cache_dir is not None:
            self.baseptr.DiskCacheDir = <TString>disk_cache_dir.encode('utf-8')

        if localhost_port and tvmapi_url:
            raise __BrokenTvmClientSettings('localhost_port and tvmapi_url are both provided')

        if localhost_port is not None:
            self.baseptr.TvmHost = <TString>'localhost'.encode('utf-8')
            self.baseptr.TvmPort = <int>localhost_port
        if tvmapi_url:
            # TODO: set tvm-api url in C++ settings
            url = urllib3.util.parse_url(tvmapi_url)
            if not url.scheme:
                raise __BrokenTvmClientSettings('scheme in tvmapi_url cannot be empty: "%s"' % tvmapi_url)
            port = url.port or 443
            host = '{scheme}://{hostname}'.format(scheme=url.scheme, hostname=url.hostname)
            self.baseptr.TvmHost = <TString>host.encode('utf-8')
            self.baseptr.TvmPort = <int>port

        if fetch_roles_for_idm_system_slug is not None:
            self.baseptr.FetchRolesForIdmSystemSlug = <TString>fetch_roles_for_idm_system_slug.encode('utf-8')

            if tirole_tvmid:
                self.baseptr.TiroleTvmId = <int>tirole_tvmid
            if tirole_host:
                self.baseptr.TiroleHost = <TString>tirole_host.encode('utf-8')
            if tirole_port:
                self.baseptr.TirolePort = <int>tirole_port

        self.baseptr.CheckValid()


    def __dealloc__(self):
        del self.baseptr


cdef class __TvmToolClientSettings:
    cdef TTvmToolClientSettings* baseptr

    def __init__(self, *args, **kwargs):
        pass

    def __cinit__(self,
                  self_alias,
                  auth_token=None,
                  port=None,
                  hostname="localhost",
                  override_bb_env=None,
                  check_src_by_default=True,
                  check_default_uid_by_default=True,
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

        if not check_src_by_default:
            self.baseptr.ShouldCheckSrc = False

        if not check_default_uid_by_default:
            self.baseptr.ShouldCheckDefaultUid = False

    def __dealloc__(self):
        del self.baseptr


cdef class __Roles:
    cdef cshared_ptr[TRoles] baseptr

    def __init__(self, *args, **kwargs):
        pass

    def __cinit__(self):
        pass

    @property
    def meta(self):
        cdef TRoles.TMeta m = dereference(self.baseptr).GetMeta()
        return m.Revision.decode('utf-8'), m.BornTime.Seconds(), m.Applied.Seconds()

    @property
    def raw(self):
        cdef TString r = dereference(self.baseptr).GetRaw()
        return r.decode('utf-8')

    def get_service_roles(self, ticket):
        assert isinstance(ticket, __CheckedServiceTicket)

        return self._build_roles(dereference(self.baseptr).GetRolesForService(
            dereference((<__CheckedServiceTicket> ticket).baseptr.Get()),
        ))

    def get_user_roles(self, ticket, selected_uid):
        assert isinstance(ticket, __CheckedUserTicket)

        cdef TOptUid selectedUid
        if selected_uid is not None:
            selectedUid = TOptUid(<TUid>selected_uid)

        return self._build_roles(dereference(self.baseptr).GetRolesForUser(
            dereference((<__CheckedUserTicket>ticket).baseptr.Get()),
            selectedUid,
        ))

    cdef _build_roles(self, cshared_ptr[TConsumerRoles] roles):
        if roles == NULL:
            return {}

        return {
            pair.first.decode('utf-8'): [
                {
                    k.decode("utf-8"): v.decode("utf-8")
                    for k, v in dereference(entity)
                }
                for entity in dereference(pair.second).GetEntitiesWithAttrs(self._build_entity({}))
            ]
            for pair in dereference(roles).GetRoles()
        }

    def check_service_role(self, ticket, role, exact_entity):
        assert isinstance(ticket, __CheckedServiceTicket)

        if exact_entity is not None:
            return dereference(self.baseptr).CheckServiceRoleForExactEntity(
                dereference((<__CheckedServiceTicket>ticket).baseptr.Get()),
                <TString>role.encode('utf-8'),
                self._build_entity(exact_entity),
            )
        else:
            return dereference(self.baseptr).CheckServiceRole(
                dereference((<__CheckedServiceTicket>ticket).baseptr.Get()),
                <TString>role.encode('utf-8'),
            )

    def check_user_role(self, ticket, role, selected_uid, exact_entity):
        assert isinstance(ticket, __CheckedUserTicket)

        cdef TOptUid selectedUid
        if selected_uid is not None:
            selectedUid = TOptUid(<TUid>selected_uid)

        if exact_entity is not None:
            return dereference(self.baseptr).CheckUserRoleForExactEntity(
                dereference((<__CheckedUserTicket>ticket).baseptr.Get()),
                <TString>role.encode('utf-8'),
                self._build_entity(exact_entity),
                selectedUid,
            )
        else:
            return dereference(self.baseptr).CheckUserRole(
                dereference((<__CheckedUserTicket>ticket).baseptr.Get()),
                <TString>role.encode('utf-8'),
                selectedUid,
            )

    cdef TEntity _build_entity(self, exact_entity) except *:
        cdef TEntity ent
        for k, v in exact_entity.items():
            ent.insert(pair[TString, TString](k.encode('utf-8'), v.encode('utf-8')))
        return ent


cdef class __Logger:
    cdef TPyLogger.TPyLoggerPtr baseptr

    def __init__(self, *args, **kwargs):
        pass

    def __cinit__(self):
        self.baseptr = TPyLogger.Create()

    def fetch(self):
        cdef TVector[pair[int, TString]] msg = TPyLogger.FetchMessages(self.baseptr)
        return [(p.first, p.second.decode("utf-8")) for p in msg]


cdef class __TvmClient:
    cdef TPidCheckedClient* baseptr

    def __init__(self, *args, **kwargs):
        pass

    def __cinit__(self, settings, logger):
        if isinstance(settings, __TvmToolClientSettings):
            self.baseptr = CreateTvmToolClient( \
                dereference((<__TvmToolClientSettings>settings).baseptr),\
                (<__Logger>logger).baseptr)
        else:
            self.baseptr = CreateTvmApiClient( \
                dereference((<__TvmApiClientSettings>settings).baseptr),\
                (<__Logger>logger).baseptr)

    def stop(self):
        del self.baseptr
        self.baseptr = NULL

    def __dealloc__(self):
        self.stop()

    @property
    def status(self):
        self.__check()
        cdef TClientStatus s = self.baseptr.GetStatus()
        return <int>s.GetCode(), s.GetLastError().decode('utf-8')

    def get_service_ticket_for(self, alias=None, tvm_id=None):
        self.__check()
        if alias is not None:
            return self.baseptr.GetServiceTicketFor(<TString>alias.encode('utf-8')).decode('utf-8')
        if tvm_id is not None:
            return GetServiceTicketForId(dereference(self.baseptr), int(tvm_id)).decode('utf-8')
        raise __TvmException("One of args is required: 'alias' or 'tvm_id'")

    def check_service_ticket(self, ticket):
        self.__check()
        return __ServiceContext.__check(Move(
            self.baseptr.CheckServiceTicket(<TString>ticket.encode('utf-8'))))

    def check_user_ticket(self, ticket, overrided_bb_env=None):
        self.__check()
        if overrided_bb_env is None:
            return __UserContext.__check(Move(
                self.baseptr.CheckUserTicket(<TString>ticket.encode('utf-8'))))
        else:
            return __UserContext.__check(Move(
                self.baseptr.CheckUserTicketWithOveridedEnv(<TString>ticket.encode('utf-8'), <EBlackboxEnv><int>overrided_bb_env)))

    def get_roles(self):
        self.__check()
        res = __Roles()
        res.baseptr = self.baseptr.GetRoles()
        return res

    def __check(self):
        if NULL == self.baseptr:
            raise __NonRetriableException("TvmClient is already stopped")

TA_InitThreads()
