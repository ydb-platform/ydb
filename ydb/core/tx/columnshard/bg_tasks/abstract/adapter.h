#pragma once
#include "session.h"
#include <ydb/core/tablet_flat/tablet_flat_executor.h>
#include <ydb/core/tx/columnshard/common/tablet_id.h>

#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/accessor/accessor.h>
#include <ydb/library/conclusion/status.h>

namespace NKikimr::NTabletFlatExecutor {
class TTabletExecutedFlat;
}

namespace NKikimr::NOlap::NBackground {

class ITabletAdapter {
private:
    YDB_READONLY_DEF(NActors::TActorId, TabletActorId);
    YDB_READONLY(TTabletId, TabletId, TTabletId(0));
    NTabletFlatExecutor::TTabletExecutedFlat& TabletExecutor;
    virtual bool DoLoadSessionsFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, std::deque<TSessionRecord>& records) = 0;
    virtual void DoSaveProgressToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual void DoSaveStateToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual void DoSaveSessionToLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TSessionRecord& container) = 0;
    virtual void DoRemoveSessionFromLocalDatabase(NTabletFlatExecutor::TTransactionContext& txc, const TString& className, const TString& identifier) = 0;
public:
    ITabletAdapter(const NActors::TActorId& tabletActorId, const TTabletId tabletId, NTabletFlatExecutor::TTabletExecutedFlat& tabletExecutor)
        : TabletActorId(tabletActorId)
        , TabletId(tabletId)
        , TabletExecutor(tabletExecutor)
    {
        AFL_VERIFY(!!TabletActorId);
        AFL_VERIFY(!!(ui64)TabletId);
    }
    virtual ~ITabletAdapter() = default;

    template <class T>
    T& GetTabletExecutorVerifiedAs() {
        T* result = dynamic_cast<T*>(&TabletExecutor);
        AFL_VERIFY(result);
        return *result;
    }

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