#pragma once
#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/actorid.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/actor_virtual.h>
#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/core/base/events.h>

namespace NKikimr::NMetadata::NRequest {

enum EEvents {
    EvCreateTableRequest = EventSpaceBegin(TKikimrEvents::ES_INTERNAL_REQUEST),
    EvCreateTableInternalResponse,
    EvCreateTableResponse,

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
    virtual void OnRequestFailed(const TString& errorMessage) = 0;
};

using TDialogCreateTable = TDialogPolicyImpl<Ydb::Table::CreateTableRequest, Ydb::Table::CreateTableResponse,
    EEvents::EvCreateTableRequest, EEvents::EvCreateTableInternalResponse, EEvents::EvCreateTableResponse>;
using TDialogDropTable = TDialogPolicyImpl<Ydb::Table::DropTableRequest, Ydb::Table::DropTableResponse,
    EEvents::EvDropTableRequest, EEvents::EvDropTableInternalResponse, EEvents::EvDropTableResponse>;
using TDialogModifyPermissions = TDialogPolicyImpl<Ydb::Scheme::ModifyPermissionsRequest, Ydb::Scheme::ModifyPermissionsResponse,
    EEvents::EvModifyPermissionsRequest, EEvents::EvModifyPermissionsInternalResponse, EEvents::EvModifyPermissionsResponse>;
using TDialogSelect = TDialogPolicyImpl<Ydb::Table::ExecuteDataQueryRequest, Ydb::Table::ExecuteDataQueryResponse,
    EEvents::EvSelectRequest, EEvents::EvSelectInternalResponse, EEvents::EvSelectResponse>;
using TDialogCreateSession = TDialogPolicyImpl<Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse,
    EEvents::EvCreateSessionRequest, EEvents::EvCreateSessionInternalResponse, EEvents::EvCreateSessionResponse>;

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

template <class TResult>
class TMaybeResult {
private:
    YDB_READONLY_DEF(TString, ErrorMessage);
    YDB_ACCESSOR_DEF(TResult, Result);
public:
    TMaybeResult(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }

    TMaybeResult(const char* errorMessage)
        : ErrorMessage(errorMessage) {

    }

    TMaybeResult(TResult&& result)
        : Result(std::move(result)) {

    }

    const TResult& operator*() const {
        Y_ENSURE(!ErrorMessage, yexception() << "incorrect object for result request");
        return Result;
    }

    bool operator!() const {
        return !!ErrorMessage;
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
    YDB_READONLY_DEF(TString, ErrorMessage)
public:
    TEvRequestFailed(const TString& errorMessage)
        : ErrorMessage(errorMessage) {

    }
};

class TEvRequestFinished: public NActors::TEventLocal<TEvRequestFinished, EEvents::EvRequestFinished> {
public:
};

}
