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
    class TStageCounters: NColumnShard::TCommonCountersOwner {
    private:
        using TBase = NColumnShard::TCommonCountersOwner;
        std::array<NMonitoring::TDynamicCounters::TCounterPtr, (ui64)EStage::COUNT> Stages;

    public:
        TStageCounters(const NColumnShard::TCommonCountersOwner& owner, const NBlobOperations::EConsumer consumerId)
            : TBase(owner, "consumer", ToString(consumerId)) {
            for (size_t i = 0; i < (ui64)EStage::COUNT; ++i) {
                Stages[i] = TBase::GetDeriviative(TStringBuilder() << "Changes/Stage/" << static_cast<EStage>(i));
            }
        }

        void OnStageChanged(const EStage stage) const {
            AFL_VERIFY(static_cast<size_t>(stage) < Stages.size())("index", stage)("size", Stages.size());
            Stages[static_cast<size_t>(stage)]->Inc();
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
            StagesByConsumer[i] = std::make_shared<TStageCounters>(*this, i);
        }
    }

    static std::shared_ptr<TStageCounters> GetStageCounters(const NBlobOperations::EConsumer consumerId) {
        return Singleton<TChangesCounters>()->GetStageCountersImpl(consumerId);
    }
};

}   // namespace NKikimr::NOlap::NChanges
