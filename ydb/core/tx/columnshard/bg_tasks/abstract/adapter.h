#pragma once
#include "session.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NBackground {

class ITabletAdapter {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    virtual bool DoLoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<TSessionRecord>& records) = 0;
    virtual TConclusionStatus DoSaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual TConclusionStatus DoSaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual TConclusionStatus DoSaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual TConclusionStatus DoRemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) = 0;
public:
    ITabletAdapter(const NActors::TActorId& actorId)
        : TabletActorId(actorId)
    {

    }

    [[nodiscard]] TConclusionStatus RemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) {
        return DoRemoveSessionFromLocalDatabase(txc, className, identifier);
    }

    [[nodiscard]] bool LoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<TSessionRecord>& records) {
        return DoLoadSessionsFromLocalDatabase(txc, records);
    }

    [[nodiscard]] TConclusionStatus SaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& session) {
        return DoSaveSessionToLocalDatabase(txc, session);
    }

    [[nodiscard]] TConclusionStatus SaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& session) {
        return DoSaveStateToLocalDatabase(txc, session);
    }

    [[nodiscard]] TConclusionStatus SaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& session) {
        return DoSaveProgressToLocalDatabase(txc, session);
    }
};

}