#pragma once

#include <ydb/core/tx/columnshard/blobs_action/counters/storage.h>
#include <ydb/core/tx/columnshard/counters/common/owner.h>

#include <ydb/library/actors/core/log.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/generic/hash.h>

namespace NKikimr::NOlap::NChanges {

enum class EStage : ui32 {
    Created = 0,
    Started,
    Constructed,
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
        NMonitoring::TDynamicCounters::TCounterPtr Tasks;
        NMonitoring::TDynamicCounters::TCounterPtr WritePortions;

    public:
        void Inc(const ui64 writePortions) const {
            Tasks->Inc();
            WritePortions->Add(writePortions);
        }

        TTaskCounters(const NColumnShard::TCommonCountersOwner& owner, const TString& sensorPrefix)
            : TBase(owner)
            , Tasks(TBase::GetDeriviative(TStringBuilder() << sensorPrefix << "Tasks"))
            , WritePortions(TBase::GetDeriviative(TStringBuilder() << sensorPrefix << "WritePortions")) {
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
                Stages[i] = std::make_shared<TTaskCounters>(TBase::CreateSubGroup("stage", ToString(static_cast<EStage>(i))), "Changes/Stage/");
            }
        }

        void OnStageChanged(const EStage stage, const ui64 writePortions) const {
            AFL_VERIFY(static_cast<size_t>(stage) < Stages.size())("index", stage)("size", Stages.size());
            // coverity[overrun-call]
            Stages[static_cast<size_t>(stage)]->Inc(writePortions);
        }
    };

private:
    std::array<std::shared_ptr<TStageCounters>, static_cast<size_t>(NBlobOperations::EConsumer::COUNT)> StagesByConsumer;

    std::shared_ptr<TStageCounters> GetStageCountersImpl(const NBlobOperations::EConsumer consumerId) {
        AFL_VERIFY((ui64)consumerId < StagesByConsumer.size())("index", consumerId)("size", StagesByConsumer.size());
        return StagesByConsumer[(ui64)consumerId];
    }

public:
    TChangesCounters()
        : TBase("ColumnEngineChanges") {
        for (ui64 i = 0; i < (ui64)NBlobOperations::EConsumer::COUNT; ++i) {
            StagesByConsumer[i] = std::make_shared<TStageCounters>(*this, static_cast<NBlobOperations::EConsumer>(i));
        }
    }

    static std::shared_ptr<TStageCounters> GetStageCounters(const NBlobOperations::EConsumer consumerId) {
        return Singleton<TChangesCounters>()->GetStageCountersImpl(consumerId);
    }
};

}   // namespace NKikimr::NOlap::NChanges
