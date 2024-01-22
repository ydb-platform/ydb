#pragma once
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/actor_virtual.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NMetadata::NRequest {

enum EEvents {
    EvCreateTableRequest = EventSpaceBegin(TKikimrEvents::ES_INTERNAL_REQUEST),
    EvCreateTableInternalResponse,
    EvCreateTableResponse,

    EvAlterTableRequest,
    EvAlterTableInternalResponse,
    EvAlterTableResponse,

    EvDropTableRequest,
    EvDropTableInternalResponse,
    EvDropTableResponse,

    EvSelectRequest,
    EvSelectInternalResponse,
    EvSelectResponse,

    EvYQLRequest,
    EvYQLInternalResponse,
    EvGeneralYQLResponse,

    EvCreateSessionRequest,
    EvCreateSessionInternalResponse,
    EvCreateSessionResponse,

    EvModifyPermissionsRequest,
    EvModifyPermissionsInternalResponse,
    EvModifyPermissionsResponse,

    EvRequestFinished,
    EvRequestFailed,
    EvRequestStart,

    EvCreatePathRequest,
    EvCreatePathInternalResponse,
    EvCreatePathResponse,

    EvDeleteSessionRequest,
    EvDeleteSessionInternalResponse,
    EvDeleteSessionResponse,

    EvEnd
};

static_assert(EEvents::EvEnd < EventSpaceEnd(TKikimrEvents::ES_INTERNAL_REQUEST), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_INTERNAL_REQUESTS)");

// class with interaction features for ydb-yql request execution
template <class TRequestExt, class TResponseExt, ui32 EvStartExt, ui32 EvResultInternalExt, ui32 EvResultExt>
class TDialogPolicyImpl {
public:
    using TRequest = TRequestExt;
    using TResponse = TResponseExt;
    static constexpr ui32 EvStart = EvStartExt;
    static constexpr ui32 EvResultInternal = EvResultInternalExt;
    static constexpr ui32 EvResult = EvResultExt;
};

template <class TExtDialogPolicy>
class IExternalController {
public:
    using TDialogPolicy = TExtDialogPolicy;
    using TPtr = std::shared_ptr<IExternalController>;
    virtual ~IExternalController() = default;
    virtual void OnRequestResult(typename TDialogPolicy::TResponse&& result) = 0;
    virtual void OnRequestFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage) = 0;
};

using TDialogCreatePath = TDialogPolicyImpl<Ydb::Scheme::MakeDirectoryRequest, Ydb::Scheme::MakeDirectoryResponse,
    EEvents::EvCreatePathRequest, EEvents::EvCreatePathInternalResponse, EEvents::EvCreatePathResponse>;
using TDialogCreateTable = TDialogPolicyImpl<Ydb::Table::CreateTableRequest, Ydb::Table::CreateTableResponse,
    EEvents::EvCreateTableRequest, EEvents::EvCreateTableInternalResponse, EEvents::EvCreateTableResponse>;
using TDialogAlterTable = TDialogPolicyImpl<Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse,
    EEvents::EvAlterTableRequest, EEvents::EvAlterTableInternalResponse, EEvents::EvAlterTableResponse>;
using TDialogDropTable = TDialogPolicyImpl<Ydb::Table::DropTableRequest, Ydb::Table::DropTableResponse,
    EEvents::EvDropTableRequest, EEvents::EvDropTableInternalResponse, EEvents::EvDropTableResponse>;
using TDialogModifyPermissions = TDialogPolicyImpl<Ydb::Scheme::ModifyPermissionsRequest, Ydb::Scheme::ModifyPermissionsResponse,
    EEvents::EvModifyPermissionsRequest, EEvents::EvModifyPermissionsInternalResponse, EEvents::EvModifyPermissionsResponse>;
using TDialogSelect = TDialogPolicyImpl<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse,
    EEvents::EvSelectRequest, EEvents::EvSelectInternalResponse, EEvents::EvSelectResponse>;
using TDialogCreateSession = TDialogPolicyImpl<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse,
    EEvents::EvCreateSessionRequest, EEvents::EvCreateSessionInternalResponse, EEvents::EvCreateSessionResponse>;
using TDialogDeleteSession = TDialogPolicyImpl<Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse,
    EEvents::EvDeleteSessionRequest, EEvents::EvDeleteSessionInternalResponse, EEvents::EvDeleteSessionResponse>;

template <ui32 evResult = EEvents::EvGeneralYQLResponse>
using TCustomDialogYQLRequest = TDialogPolicyImpl<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse,
    EEvents::EvYQLRequest, EEvents::EvYQLInternalResponse, evResult>;
template <ui32 evResult = EEvents::EvCreateSessionResponse>
using TCustomDialogCreateSpecialSession = TDialogPolicyImpl<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse,
    EEvents::EvCreateSessionRequest, EEvents::EvCreateSessionInternalResponse, evResult>;

using TDialogYQLRequest = TCustomDialogYQLRequest<EEvents::EvGeneralYQLResponse>;
using TDialogCreateSpecialSession = TCustomDialogCreateSpecialSession<EEvents::EvCreateSessionResponse>;

template <class TResponse>
class TOperatorChecker {
public:
    static bool IsSuccess(const TResponse& r) {
        return r.operation().status() == Ydb::StatusIds::SUCCESS;
    }
};

template <>
class TOperatorChecker<Ydb::Scheme::MakeDirectoryResponse> {
public:
    static bool IsSuccess(const Ydb::Scheme::MakeDirectoryResponse& r) {
        return r.operation().status() == Ydb::StatusIds::SUCCESS ||
            r.operation().status() == Ydb::StatusIds::ALREADY_EXISTS;
    }
};

template <>
class TOperatorChecker<Ydb::Table::CreateTableResponse> {
public:
    static bool IsSuccess(const Ydb::Table::CreateTableResponse& r) {
        return r.operation().status() == Ydb::StatusIds::SUCCESS ||
            r.operation().status() == Ydb::StatusIds::ALREADY_EXISTS;
    }
};

template <>
class TOperatorChecker<Ydb::Table::DropTableResponse> {
public:
    static bool IsSuccess(const Ydb::Table::DropTableResponse& r) {
        return r.operation().status() == Ydb::StatusIds::SUCCESS ||
            r.operation().status() == Ydb::StatusIds::NOT_FOUND;
    }
};

template <class TDialogPolicy>
class TEvRequestResult: public NActors::TEventLocal<TEvRequestResult<TDialogPolicy>, TDialogPolicy::EvResult> {
private:
    YDB_READONLY_DEF(typename TDialogPolicy::TResponse, Result);
public:
    typename TDialogPolicy::TResponse&& DetachResult() {
        return std::move(Result);
    }

    TEvRequestResult(typename TDialogPolicy::TResponse&& result)
        : Result(std::move(result)) {

    }
};

class TEvRequestFailed: public NActors::TEventLocal<TEvRequestFailed, EEvents::EvRequestFailed> {
private:
    YDB_READONLY_DEF(Ydb::StatusIds::StatusCode, Status);
    YDB_READONLY_DEF(TString, ErrorMessage)
public:
    TEvRequestFailed(Ydb::StatusIds::StatusCode status, const TString& errorMessage)
        : Status(status)
        , ErrorMessage(errorMessage)
    {
    }
};

class TEvRequestFinished: public NActors::TEventLocal<TEvRequestFinished, EEvents::EvRequestFinished> {
public:
};

}
