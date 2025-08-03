#pragma once

#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/owner.h>
#include <ydb/library/signals/states.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NChanges {

enum class EStage : ui32 {
    Created = 0,
    Started,
    AskAccessorResources,
    AskAccessors,
    AskDataResources,
    ReadBlobs,
    ReadyForConstruct,
    Constructed,
    WriteDraft,
    AskDiskQuota,
    Writing,
    Compiled,
    Written,
    Finished,
    Aborted,

    COUNT
};

class TChangesCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    class TTaskCounters: NColumnShard::TCommonCountersOwner {
        using TBase = NColumnShard::TCommonCountersOwner;

    private:
        NMonitoring::TDynamicCounters::TCounterPtr TasksChanges;
        NMonitoring::TDynamicCounters::TCounterPtr TasksCount;

    public:
        void Inc() const {
            TasksChanges->Inc();
            TasksCount->Inc();
        }

        void Dec() const {
            TasksChanges->Dec();
            TasksCount->Dec();
        }

        TTaskCounters(const NColumnShard::TCommonCountersOwner& owner)
            : TBase(owner)
            , TasksChanges(TBase::GetDeriviative("Tasks/Count"))
            , TasksCount(TBase::GetValue("Tasks/Count")) {
        }
    };

private:
    std::array<std::shared_ptr<NCounters::TStateSignalsOperator<EStage>>, static_cast<size_t>(NBlobOperations::EConsumer::COUNT)> StagesByConsumer;

    std::shared_ptr<NCounters::TStateSignalsOperator<EStage>> GetStageCountersImpl(const NBlobOperations::EConsumer consumerId) {
        AFL_VERIFY((ui64)consumerId < StagesByConsumer.size())("index", consumerId)("size", StagesByConsumer.size());
        return StagesByConsumer[(ui64)consumerId];
    }

public:
    TChangesCounters()
        : TBase("ColumnEngineChanges") {
        for (ui64 i = 0; i < (ui64)NBlobOperations::EConsumer::COUNT; ++i) {
            auto base = this->CreateSubGroup("consumer", ::ToString(static_cast<NBlobOperations::EConsumer>(i)));
            StagesByConsumer[i] = std::make_shared<NCounters::TStateSignalsOperator<EStage>>(
                base, "indexation_stage");
        }
    }

    static NCounters::TStateSignalsOperator<EStage>::TGuard GetStageCounters(const NBlobOperations::EConsumer consumerId) {
        return Singleton<TChangesCounters>()->GetStageCountersImpl(consumerId)->BuildGuard(EStage::Created);
    }
};

}   // namespace NKikimr::NOlap::NChanges
