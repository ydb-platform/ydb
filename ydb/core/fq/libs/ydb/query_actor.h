#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/events.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/adapters/issue/issue.h>

namespace NFq {

struct TEvQuerySession {
    // Event ids
    enum EEv : ui32 {
        EvBegin = EventSpaceBegin(NActors::TEvents::ES_PRIVATE),
        EvExecuteDataQuery = EvBegin + 20,
        EvRollback,
        EvEnd
    };

    struct TTxControl {
        bool Begin = false;
        bool Commit = false;
        bool Continue = false;
        bool SnapshotRead = false;
    };

    static_assert(EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE), "expect EvEnd < EventSpaceEnd(NActors::TEvents::ES_PRIVATE)");
    struct TEvExecuteDataQuery : NActors::TEventLocal<TEvExecuteDataQuery, EvExecuteDataQuery> {
        TEvExecuteDataQuery(
            const TString& sql,
            std::shared_ptr<NYdb::TParamsBuilder> params,
            const TTxControl& txControl,
            const NYdb::NTable::TExecDataQuerySettings& execDataQuerySettings,
            NThreading::TPromise<NYdb::NTable::TDataQueryResult> promise)
            : Sql(sql)
            , Params(params)
            , TxControl(txControl)
            , ExecDataQuerySettings(execDataQuerySettings)
            , Promise(promise)
        {}
        const TString Sql;
        std::shared_ptr<NYdb::TParamsBuilder> Params;
        TTxControl TxControl;
        NYdb::NTable::TExecDataQuerySettings ExecDataQuerySettings;
        NThreading::TPromise<NYdb::NTable::TDataQueryResult> Promise;
    };
    struct TEvRollbackTransaction : public NActors::TEventLocal<TEvRollbackTransaction, EvRollback> {
    };
};

std::unique_ptr<NActors::IActor> MakeQuerySession();

} // namespace NFq
