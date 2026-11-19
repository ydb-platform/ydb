#pragma once

#include "source_rows_observer.h"

#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <ydb/library/accessor/accessor.h>

namespace NKikimr::NOlap::NStats::NScan {

class TCursorObserver {
    YDB_READONLY_DEF(std::shared_ptr<TOperationScope>, MetastepStatistics);
    YDB_READONLY_DEF(std::shared_ptr<TOperationScope>, ReaderStatistics);
    YDB_READONLY_MUTABLE_DEF(std::shared_ptr<TOperationScope>, StepStatistics);
    NColumnShardStats::TOperation::DetailsCase Metastep = NColumnShardStats::TOperation::DETAILS_NOT_SET;
    bool MetastepProducesRows = true;

public:
    template <class TSource>
    void OnReaderStart(TSource& source) {
        auto* local = source.GetLocalStats();
        if (!local) {
            return;
        }
        if (!ReaderStatistics || !*ReaderStatistics) {
            auto scope = std::make_shared<TOperationScope>();
            *scope = local->Measure(&local->GetRoot());
            ReaderStatistics = std::move(scope);
        }
        ReaderStatistics->Resume();
        source.SetActiveStatistics(ReaderStatistics->GetHandle());
    }

    template <class TSource>
    void OnMetastepStart(
        TSource& source, std::optional<NColumnShardStats::TOperation::DetailsCase> explicitKind, bool fetchingStage, bool producesRows) {
        auto* local = source.GetLocalStats();
        if (!local) {
            return;
        }
        auto kind = NColumnShardStats::TOperation::kPKFilter;
        if (source.GetType() == TSource::EType::SimpleAggregation) {
            kind = NColumnShardStats::TOperation::kAggregation;
        } else if (source.GetType() == TSource::EType::SimpleSysInfo) {
            kind = NColumnShardStats::TOperation::kFetchColumns;
        } else if (source.GetType() == TSource::EType::Undefined) {
            kind = NColumnShardStats::TOperation::kReader;
        }
        const bool plain = source.GetType() == TSource::EType::PlainPortion;
        if (explicitKind) {
            kind = *explicitKind;
        } else if (!plain && fetchingStage) {
            kind = NColumnShardStats::TOperation::kFetchColumns;
        }
        if (Metastep != kind || MetastepProducesRows != producesRows) {
            OnMetastepFinish(source);
        }
        Metastep = kind;
        MetastepProducesRows = producesRows;
        if (kind == NColumnShardStats::TOperation::kReader) {
            source.SetActiveStatistics(ReaderStatistics ? ReaderStatistics->GetHandle() : TCallHandle());
            return;
        }
        if (!MetastepStatistics || !*MetastepStatistics) {
            auto definition = MakeMetastepStatisticsDefinition(kind);
            definition.MutableStatistics()->MutableInputRows()->SetApplicability(
                producesRows ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
            definition.MutableStatistics()->MutableOutputRows()->SetApplicability(
                !producesRows || (plain && kind == NColumnShardStats::TOperation::kPKFilter) ? NColumnShardStats::NOT_APPLICABLE
                                                                                             : NColumnShardStats::APPLICABLE);
            if (kind == NColumnShardStats::TOperation::kBuildResult) {
                definition.MutableBuildResult()->MutableBatches()->SetApplicability(
                    producesRows ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
            }
            auto scope = std::make_shared<TOperationScope>();
            *scope =
                local->Measure(local->AddChild(local->GetRoot(), definition), ReaderStatistics ? ReaderStatistics->GetHandle() : TCallHandle());
            MetastepStatistics = std::move(scope);
            MetastepStatistics->Observe([&](auto& observation) {
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows)
                    .SetApplicability(producesRows ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows)
                    .SetApplicability(!producesRows || (plain && kind == NColumnShardStats::TOperation::kPKFilter)
                                          ? NColumnShardStats::NOT_APPLICABLE
                                          : NColumnShardStats::APPLICABLE);
                if (kind == NColumnShardStats::TOperation::kBuildResult) {
                    observation.Metric(&NColumnShardStats::TBuildResultDetails::MutableBatches)
                        .SetApplicability(producesRows ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
                }
            });
            if (kind == NColumnShardStats::TOperation::kPKFilter &&
                (source.GetType() == TSource::EType::SimplePortion || source.GetType() == TSource::EType::PlainPortion)) {
                MetastepStatistics->Observe([&](auto& observation) {
                    observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(source.GetRecordsCount());
                });
            } else if (producesRows && kind != NColumnShardStats::TOperation::kBuildResult &&
                       kind != NColumnShardStats::TOperation::kAggregation) {
                TSourceRowsObserver(source).OnInputRows(*MetastepStatistics);
            }
        }
        MetastepStatistics->Resume();
        source.SetActiveStatistics(MetastepStatistics->GetHandle());
    }

    template <class TSource>
    void OnOutput(TSource& source) {
        if (!MetastepStatistics || !*MetastepStatistics) {
            return;
        }
        if (Metastep == NColumnShardStats::TOperation::kBuildResult || Metastep == NColumnShardStats::TOperation::kReader ||
            Metastep == NColumnShardStats::TOperation::kAggregation ||
            (Metastep == NColumnShardStats::TOperation::kPKFilter && source.GetType() == TSource::EType::PlainPortion)) {
            return;
        }
        MetastepStatistics->Observe([](auto& observation) {
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).ClearValue();
        });
        const TSourceRowsObserver observer(source);
        if (source.HasStageData()) {
            observer.OnOutputRows(*MetastepStatistics);
        } else {
            observer.OnStageResult(*MetastepStatistics);
        }
    }

    void OnMetastepFinish() {
        if (MetastepStatistics) {
            MetastepStatistics->Finish();
            MetastepStatistics.reset();
        }
        Metastep = NColumnShardStats::TOperation::DETAILS_NOT_SET;
    }

    template <class TSource>
    void OnMetastepFinish(TSource& source) {
        if (MetastepStatistics) {
            OnOutput(source);
        }
        OnMetastepFinish();
    }

    void SetStepStatistics(std::shared_ptr<TOperationScope> scope) const noexcept {
        StepStatistics = std::move(scope);
    }

    void OnStepFinish() const {
        if (StepStatistics) {
            StepStatistics->Finish();
            StepStatistics.reset();
        }
    }

    void OnPhaseComplete() const {
        if (MetastepStatistics) {
            MetastepStatistics->Finish();
        }
    }

    void OnScriptComplete() const {
        OnStepFinish();
        OnPhaseComplete();
        if (ReaderStatistics) {
            ReaderStatistics->Finish();
        }
    }

    void OnNext(bool next) {
        if (!next) {
            OnMetastepFinish();
            if (ReaderStatistics) {
                ReaderStatistics->Finish();
            }
        } else {
            OnSuspend();
        }
    }

    void OnSuspend() const {
        if (MetastepStatistics) {
            MetastepStatistics->Suspend();
        }
        if (ReaderStatistics) {
            ReaderStatistics->Suspend();
        }
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
