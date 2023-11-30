#pragma once

#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/event_local.h>

#include <ydb/public/sdk/cpp/client/ydb_table/table.h>

namespace NDbPool {

struct TEvents {

    enum EEv {
        EvDbRequest = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvDbResponse,
        EvDbFunctionRequest,
        EvDbFunctionResponse,
        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    
    struct TEvDbRequest : NActors::TEventLocal<TEvDbRequest, EvDbRequest> {
        TString Sql;
        NYdb::TParams Params;
        bool Idempotent;

        TEvDbRequest(const TString& sql, NYdb::TParams&& params, bool idempotent = true)
            : Sql(sql)
            , Params(std::move(params))
            , Idempotent(idempotent)
        {}
    };

    struct TEvDbResponse : NActors::TEventLocal<TEvDbResponse, EvDbResponse> {
        NYdb::TStatus Status;
        TVector<NYdb::TResultSet> ResultSets;

        TEvDbResponse(NYdb::TStatus status, const TVector<NYdb::TResultSet>& resultSets)
            : Status(status)
            , ResultSets(resultSets)
        {}
    };

    struct TEvDbFunctionRequest : NActors::TEventLocal<TEvDbFunctionRequest, EvDbFunctionRequest> {
        using TFunction = std::function<NYdb::TAsyncStatus(NYdb::NTable::TSession&)>;
        TFunction Handler;

        explicit TEvDbFunctionRequest(const TFunction& handler)
            : Handler(handler)
        {}
    };

    struct TEvDbFunctionResponse : NActors::TEventLocal<TEvDbFunctionResponse, EvDbFunctionResponse> {
        NYdb::TStatus Status;

        explicit TEvDbFunctionResponse(NYdb::TStatus status)
            : Status(status)
        {}
    };

};

} // namespace NDbPool
