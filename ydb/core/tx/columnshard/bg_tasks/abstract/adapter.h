#pragma once
#include "session.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NOlap::NBackground {

class ITabletAdapter {
private:
    virtual bool DoLoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<TSessionRecord>& records) = 0;
    virtual void DoSaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual void DoSaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual void DoSaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual void DoRemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) = 0;
public:
    virtual ~ITabletAdapter() = default;

    void RemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) {
        return DoRemoveSessionFromLocalDatabase(txc, className, identifier);
    }

    [[nodiscard]] bool LoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<TSessionRecord>& records) {
        return DoLoadSessionsFromLocalDatabase(txc, records);
    }

    void SaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& session) {
        return DoSaveSessionToLocalDatabase(txc, session);
    }

    void SaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& session) {
        return DoSaveStateToLocalDatabase(txc, session);
    }

    void SaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& session) {
        return DoSaveProgressToLocalDatabase(txc, session);
    }
};

}