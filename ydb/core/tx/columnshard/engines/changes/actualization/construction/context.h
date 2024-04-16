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
    YDB_ACCESSOR(ui64, TxWriteVolume, 0);
public:
    TTaskConstructor(const std::shared_ptr<TColumnEngineChanges::IMemoryPredictor>& predictor, const std::shared_ptr<TTTLColumnEngineChanges>& task)
        : MemoryPredictor(predictor)
        , Task(task) {

    }
};

class TTieringProcessContext {
private:
    const ui64 MemoryUsageLimit;
    TSaverContext SaverContext;
    THashMap<TRWAddress, std::vector<TTaskConstructor>> Tasks;
    const NColumnShard::TEngineLogsCounters Counters;
    std::shared_ptr<NActualizer::TController> Controller;
public:
    const std::shared_ptr<NDataLocks::TManager> DataLocksManager;
    const TInstant Now = AppDataVerified().TimeProvider->Now();

    const NColumnShard::TEngineLogsCounters GetCounters() const {
        return Counters;
    }

    const THashMap<TRWAddress, std::vector<TTaskConstructor>>& GetTasks() const {
        return Tasks;
    }

    bool AddPortion(const TPortionInfo& info, TPortionEvictionFeatures&& features, const std::optional<TDuration> dWait, const TInstant now);

    bool IsRWAddressAvailable(const TRWAddress& address) const {
        auto it = Tasks.find(address);
        if (it == Tasks.end()) {
            return Controller->IsNewTaskAvailable(address, 0);
        } else {
            return Controller->IsNewTaskAvailable(address, it->second.size());
        }
    }

    TTieringProcessContext(const ui64 memoryUsageLimit, const TSaverContext& saverContext, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager,
        const NColumnShard::TEngineLogsCounters& counters, const std::shared_ptr<TController>& controller);
};

}