#pragma once

#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/library/signals/owner.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NChanges {

enum class EStage : ui32 {
    Created = 0,
    Started,
    AskResources,
    AskAccessors,
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

    class TStageCounters: NColumnShard::TCommonCountersOwner {
    private:
        using TBase = NColumnShard::TCommonCountersOwner;
        std::array<std::shared_ptr<TTaskCounters>, (ui64)EStage::COUNT> Stages;

    public:
        TStageCounters(const NColumnShard::TCommonCountersOwner& owner, const NBlobOperations::EConsumer consumerId)
            : TBase(owner, "consumer", ToString(consumerId)) {
            for (size_t i = 0; i < (ui64)EStage::COUNT; ++i) {
                Stages[i] = std::make_shared<TTaskCounters>(TBase::CreateSubGroup("stage", ToString(static_cast<EStage>(i))));
            }
        }

        void OnStageChanged(const std::optional<EStage> stageFrom, const std::optional<EStage> stageTo) const {
            if (stageFrom) {
                AFL_VERIFY(static_cast<size_t>(*stageFrom) < Stages.size())("index", *stageFrom)("size", Stages.size());
                Stages[static_cast<size_t>(*stageFrom)]->Dec();
            }
            if (stageTo) {
                AFL_VERIFY(static_cast<size_t>(*stageTo) < Stages.size())("index", *stageTo)("size", Stages.size());
                Stages[static_cast<size_t>(*stageTo)]->Inc();
            }
        }
    };

private:
    std::array<std::shared_ptr<TStageCounters>, static_cast<size_t>(NBlobOperations::EConsumer::COUNT)> StagesByConsumer;

    std::shared_ptr<TStageCounters> GetStageCountersImpl(const NBlobOperations::EConsumer consumerId) {
        AFL_VERIFY((ui64)consumerId < StagesByConsumer.size())("index", consumerId)("size", StagesByConsumer.size());
        return StagesByConsumer[(ui64)consumerId];
    }

public:
    class TStageCountersGuard: TNonCopyable {
    private:
        const std::shared_ptr<TStageCounters> Counters;
        YDB_READONLY(EStage, CurrentStage, EStage::Created);
    public:
        TStageCountersGuard(const std::shared_ptr<TStageCounters>& counters, const EStage startStage)
            : Counters(counters)
            , CurrentStage(startStage) {
            Counters->OnStageChanged(std::nullopt, startStage);
        }

        ~TStageCountersGuard() {
            Counters->OnStageChanged(CurrentStage, std::nullopt);
        }

        void SetStage(const EStage stageTo) const {
            AFL_VERIFY(stageTo >= CurrentStage)("current", CurrentStage)("to", stageTo);
            if (CurrentStage != stage) {
                Counters->OnStageChanged(CurrentStage, stageTo);
            }
        }
    };

    TChangesCounters()
        : TBase("ColumnEngineChanges") {
        for (ui64 i = 0; i < (ui64)NBlobOperations::EConsumer::COUNT; ++i) {
            StagesByConsumer[i] = std::make_shared<TStageCounters>(*this, static_cast<NBlobOperations::EConsumer>(i));
        }
    }

    static std::shared_ptr<TStageCountersGuard> GetStageCounters(const NBlobOperations::EConsumer consumerId) {
        return std::make_shared<TStageCountersGuard>(Singleton<TChangesCounters>()->GetStageCountersImpl(consumerId), EStage::Created);
    }
};

}   // namespace NKikimr::NOlap::NChanges
