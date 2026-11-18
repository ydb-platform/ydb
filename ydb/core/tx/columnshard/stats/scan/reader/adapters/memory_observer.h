#pragma once

#include "source_scope_factory.h"

#include <util/string/builder.h>

namespace NKikimr::NOlap::NStats::NScan {

class TMemoryReservationObserver {
    std::shared_ptr<TOperationScope> Scope;

    explicit TMemoryReservationObserver(std::shared_ptr<TOperationScope> scope)
        : Scope(std::move(scope))
    {
    }

public:
    TMemoryReservationObserver() = default;

    template <class TSource, class TCursor, class TStage>
    static TMemoryReservationObserver ForSource(const TSource& source, const TCursor& cursor, TStage stage, bool forceReaderParent = false) {
        return TMemoryReservationObserver(
            TSourceScopeFactory(source).StartStep(cursor, NColumnShardStats::TOperation::kReserveMemory, "Memory", [&](auto& definition) {
                definition.SetKey(TStringBuilder() << "Memory:" << static_cast<ui32>(stage));
                definition.MutableReserveMemory()->MutableRequestedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
                definition.MutableReserveMemory()->MutableQuotaBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
                definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
            }, false, forceReaderParent));
    }

    TCallHandle GetCall() const {
        return Scope ? Scope->GetHandle() : TCallHandle();
    }

    void OnRequested(ui64 bytes) {
        if (Scope) {
            Scope->Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TReserveMemoryDetails::MutableRequestedBytes).SetValue(bytes);
            });
            Scope->Suspend();
        }
    }

    void OnAllocated(ui64 bytes) {
        if (Scope) {
            Scope->Resume();
            Scope->Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TReserveMemoryDetails::MutableQuotaBytes).SetValue(bytes);
            });
            Scope->Finish();
        }
    }

    void OnAllocationImpossible() {
        OnAllocated(0);
    }
};

class TDuplicateMemoryObserver {
    TOperationScope Reader;
    TOperationScope PKFilter;
    TOperationScope ReserveMemory;

public:
    void OnRequested(TLocalStats* local, ui64 requested) {
        if (!local) {
            return;
        }
        auto* pk = local->AddChild(local->GetRoot(), MakeMetastepStatisticsDefinition(NColumnShardStats::TOperation::kPKFilter));
        auto definition = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kReserveMemory, "ReserveMemory");
        definition.MutableReserveMemory()->MutableQuotaBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
        definition.MutableReserveMemory()->MutableRequestedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
        auto* reserve = pk ? local->AddChild(*pk, definition) : nullptr;
        Reader = local->Measure(&local->GetRoot());
        PKFilter = local->Measure(pk);
        ReserveMemory = local->Measure(reserve);
        ReserveMemory.Observe([&](auto& observation) {
            observation.Metric(&NColumnShardStats::TReserveMemoryDetails::MutableRequestedBytes).SetValue(requested);
        });
        for (auto* scope : { &Reader, &PKFilter, &ReserveMemory }) {
            scope->Observe([](auto& observation) {
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows)
                    .SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows)
                    .SetApplicability(NColumnShardStats::NOT_APPLICABLE);
            });
            scope->Suspend();
        }
    }

    void OnAllocated(ui64 granted) {
        Reader.Resume();
        PKFilter.Resume();
        ReserveMemory.Resume();
        ReserveMemory.Observe([&](auto& observation) {
            observation.Metric(&NColumnShardStats::TReserveMemoryDetails::MutableQuotaBytes).SetValue(granted);
        });
        ReserveMemory.Finish();
        PKFilter.Finish();
        Reader.Finish();
    }

    void OnAllocationImpossible() {
        OnAllocated(0);
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
