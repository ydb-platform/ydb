#pragma once
#include <ydb/core/tx/columnshard/counters/engine_logs.h>
#include <ydb/core/tx/columnshard/engines/changes/abstract/abstract.h>
#include <ydb/core/tx/columnshard/engines/storage/actualizer/common/address.h>
#include <ydb/core/tx/columnshard/engines/column_engine.h>
#include <ydb/core/tx/columnshard/engines/changes/ttl.h>
#include <ydb/core/tx/columnshard/engines/changes/actualization/controller/controller.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NActualizer {

class TTaskConstructor {
private:
    YDB_READONLY_DEF(std::shared_ptr<TColumnEngineChanges::IMemoryPredictor>, MemoryPredictor);
    YDB_READONLY_DEF(std::shared_ptr<TTTLColumnEngineChanges>, Task);
    YDB_ACCESSOR(ui64, MemoryUsage, 0);
    YDB_READONLY(ui64, PortionsCount, 0);
    YDB_READONLY(ui64, ChunksCount, 0);

public:
    TTaskConstructor(const std::shared_ptr<TColumnEngineChanges::IMemoryPredictor>& predictor, const std::shared_ptr<TTTLColumnEngineChanges>& task)
        : MemoryPredictor(predictor)
        , Task(task) {

    }

    bool CanTakePortionInTx(const TPortionInfo::TConstPtr& portion, const TVersionedIndex& index) {
        if (!PortionsCount) {
            return true;
        }
        return 
            (PortionsCount + 1 < 1000) &&
            (ChunksCount + portion->GetApproxChunksCount(portion->GetSchema(index)->GetColumnsCount()) < 100000);
    }

    void TakePortionInTx(const TPortionInfo::TConstPtr& portion, const TVersionedIndex& index) {
        ++PortionsCount;
        ChunksCount += portion->GetApproxChunksCount(portion->GetSchema(index)->GetColumnsCount());
    }
};

class TTieringProcessContext {
public:
    enum class EAddPortionResult {
        SUCCESS = 0,
        TASK_LIMIT_EXCEEDED,
        PORTION_LOCKED,
    };

private:
    const TVersionedIndex& VersionedIndex;
    THashSet<TPortionAddress> UsedPortions;
    const ui64 MemoryUsageLimit;
    TSaverContext SaverContext;
    THashMap<TRWAddress, std::vector<TTaskConstructor>> Tasks;
    const NColumnShard::TEngineLogsCounters Counters;
    std::shared_ptr<NActualizer::TController> Controller;
    TInstant ActualInstant = AppData()->TimeProvider->Now();
public:
    const std::shared_ptr<NDataLocks::TManager> DataLocksManager;

    TInstant GetActualInstant() const {
        return ActualInstant;
    }

    void ResetActualInstantForTest() {
        ActualInstant = TlsActivationContext ? AppData()->TimeProvider->Now() : TInstant::Now();
    }

    const NColumnShard::TEngineLogsCounters GetCounters() const {
        return Counters;
    }

    const THashMap<TRWAddress, std::vector<TTaskConstructor>>& GetTasks() const {
        return Tasks;
    }

    TString DebugString() const {
        TStringBuilder result;
        result << "{";
        for (auto&& i : Tasks) {
            result << i.first.DebugString() << ":" << i.second.size() << ";";
        }
        result << "}";
        return result;
    }

    EAddPortionResult AddPortion(const std::shared_ptr<const TPortionInfo>& info, TPortionEvictionFeatures&& features, const std::optional<TDuration> dWait);

    bool IsRWAddressAvailable(const TRWAddress& address) const {
        auto it = Tasks.find(address);
        if (it == Tasks.end()) {
            return Controller->IsNewTaskAvailable(address, 0);
        } else {
            return Controller->IsNewTaskAvailable(address, it->second.size());
        }
    }

    TTieringProcessContext(const ui64 memoryUsageLimit, const TSaverContext& saverContext,
        const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const TVersionedIndex& versionedIndex,
        const NColumnShard::TEngineLogsCounters& counters, const std::shared_ptr<TController>& controller);
};

}