#pragma once

#include "index_registry.h"
#include "source_rows_observer.h"

#include <ydb/core/formats/arrow/program/collection.h>
#include <ydb/core/tx/columnshard/engines/scheme/indexes/abstract/meta.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/abstract.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <ydb/library/accessor/accessor.h>

#include <util/string/builder.h>

namespace NKikimr::NOlap::NStats::NScan {

class TIndexDecodeObserver final: public NIndexes::IIndexDecodeObserver {
    TLocalStats* LocalStats;
    TCallHandle Parent;
    TOperationScope Scope;

public:
    TIndexDecodeObserver(TLocalStats* localStats, TCallHandle parent)
        : LocalStats(localStats)
        , Parent(std::move(parent))
    {
    }

    void OnBegin(ui64 encodedBytes, TStringBuf codec) override {
        auto* parent = Parent.GetNode();
        if (!LocalStats || !parent) {
            return;
        }
        auto definition = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kDecode, TStringBuilder() << "IndexPayload:" << codec);
        definition.MutableDecode()->SetCodec(TString(codec));
        definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        definition.MutableStatistics()->SetInputRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
        Scope = LocalStats->Measure(LocalStats->AddChild(*parent, definition), Parent);
        Scope.Observe([&](auto& observation) {
            observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputBytes).SetValue(encodedBytes);
        });
    }

    void OnEnd() override {
        Scope.Finish();
    }
};

template <class TSource>
class TIndexObserverFactory;

class TIndexCheckObserver {
    template <class TSource>
    friend class TIndexObserverFactory;

    TOperationScope Phase;
    TOperationScope Check;
    YDB_READONLY_DEF(std::optional<ui64>, InputRows);

public:
    TCallHandle GetCurrentCall() const noexcept {
        return Check.GetHandle();
    }

    void OnDataAvailable(bool available) {
        Check.Observe([&](auto& observation) {
            auto& metric = observation.Metric(&NColumnShardStats::TIndexCheckDetails::MutableDataAvailable);
            if (available) {
                metric.SetTrueCount(1);
            } else {
                metric.SetFalseCount(1);
            }
        });
    }

    void OnMatched(bool matched) {
        Check.Observe([&](auto& observation) {
            auto& metric = observation.Metric(&NColumnShardStats::TIndexCheckDetails::MutableMatched);
            metric.SetApplicability(NColumnShardStats::APPLICABLE);
            if (matched) {
                metric.SetTrueCount(1);
            } else {
                metric.SetFalseCount(1);
            }
        });
    }

    void OnMaskApplied(bool applied) {
        Check.Observe([&](auto& observation) {
            auto& metric = observation.Metric(&NColumnShardStats::TIndexCheckDetails::MutableMaskApplied);
            if (applied) {
                metric.SetTrueCount(1);
            } else {
                metric.SetFalseCount(1);
            }
        });
    }

    void OnCandidates(ui64 input, ui64 output) {
        Check.Observe([&](auto& observation) {
            auto& in = observation.Metric(
                &NColumnShardStats::TIndexCheckDetails::MutableCandidates, &NColumnShardStats::TRowFilterDescription::MutableInputRows);
            in.SetApplicability(NColumnShardStats::APPLICABLE);
            in.SetValue(input);
            auto& out = observation.Metric(
                &NColumnShardStats::TIndexCheckDetails::MutableCandidates, &NColumnShardStats::TRowFilterDescription::MutableOutputRows);
            out.SetApplicability(NColumnShardStats::APPLICABLE);
            out.SetValue(output);
        });
    }

    void OnOutputRows(std::optional<ui64> rows) {
        if (!rows) {
            return;
        }
        const auto observe = [&](auto& operation) {
            operation.Metric(&NColumnShardStats::TStatisticsDescription::MutableOutputRows).SetValue(*rows);
        };
        Phase.Observe(observe);
        Check.Observe(observe);
    }

    void OnFinished() {
        Check.Finish();
        Phase.Finish();
    }
};

template <class TSource>
class TIndexObserverFactory {
    const TSource& Source;

    static NColumnShardStats::EIndexCheckerType CheckerType(const NArrow::NSSA::TIndexCheckOperation& operation) {
        using EOperation = NArrow::NSSA::TIndexCheckOperation::EOperation;
        switch (operation.GetOperation()) {
            case EOperation::Equals:
                return NColumnShardStats::EQUALS;
            case EOperation::StartsWith:
                return NColumnShardStats::STARTS_WITH;
            case EOperation::EndsWith:
                return NColumnShardStats::ENDS_WITH;
            case EOperation::Contains:
                return NColumnShardStats::CONTAINS;
            case EOperation::Less:
                return NColumnShardStats::LESS;
            case EOperation::Greater:
                return NColumnShardStats::GREATER;
            case EOperation::LessOrEqual:
                return NColumnShardStats::LESS_OR_EQUAL;
            case EOperation::GreaterOrEqual:
                return NColumnShardStats::GREATER_OR_EQUAL;
        }
        return NColumnShardStats::CHECKER_UNKNOWN;
    }

public:
    explicit TIndexObserverFactory(const TSource& source)
        : Source(source)
    {
    }

    TOperationNode* Register(const NIndexes::IIndexMeta& index, ui32 columnId = 0) const {
        auto* registry = Source.GetIndexRegistry();
        if (!registry) {
            return nullptr;
        }
        if (auto* registered = registry->Find(index.GetIndexId())) {
            return registered;
        }
        NColumnShardStats::TIndexDescription description;
        description.SetTableId(Source.GetRawPathId());
        description.SetIndexId(index.GetIndexId());
        description.SetName(index.GetIndexName());
        NColumnShardStats::EIndexType type = NColumnShardStats::INDEX_UNKNOWN;
        const auto className = index.GetClassName();
        NColumnShardStats::EIndexType_Parse(std::string(className.data(), className.size()), &type);
        description.SetType(type);
        if (const auto id = index.GetSingleColumnId()) {
            columnId = *id;
        }
        if (columnId) {
            if (const auto* resolver = Source.GetContext()->GetCommonContext()->GetResolver()) {
                const auto name = resolver->GetColumnName(columnId, false);
                if (name) {
                    description.AddColumns(name);
                }
            }
        }
        return registry->Register(description);
    }

    TOperationScope StartAssemble(ui32 indexId) const {
        auto* local = Source.GetLocalStats();
        const auto* registry = Source.GetIndexRegistry();
        auto* index = registry ? registry->Find(indexId) : nullptr;
        if (!local || !index) {
            return {};
        }
        auto definition = MakeStepStatisticsDefinition(NColumnShardStats::TOperation::kAssemble, "IndexPayload");
        for (const auto& column : index->GetDefinition().GetIndexFilter().GetIndex().GetColumns()) {
            definition.MutableAssemble()->AddColumns(column);
        }
        definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        return local->Measure(local->AddChild(*index, definition));
    }

    TIndexDecodeObserver CreateDecodeObserver(TCallHandle parent) const {
        return TIndexDecodeObserver(Source.GetLocalStats(), std::move(parent));
    }

    TIndexCheckObserver StartCheck(const NIndexes::IIndexMeta* index, ui32 columnId, const NArrow::NSSA::TIndexCheckOperation& operation,
        const std::shared_ptr<arrow::Scalar>& value, const NArrow::NAccessor::TAccessorsCollection& resources) const {
        TIndexCheckObserver result;
        auto* local = Source.GetLocalStats();
        if (!local) {
            return result;
        }
        auto* registry = Source.GetIndexRegistry();
        if (!registry) {
            return result;
        }
        auto* parent = &local->GetRoot();
        auto* indexNode = index ? Register(*index, columnId) : nullptr;
        if (index) {
            if (!indexNode) {
                return result;
            }
            parent = indexNode;
            result.Phase = local->Measure(parent, Source.GetCurrentStatisticsCall());
        }
        const ui32 order = registry->TakeApplicationOrder();
        TString probe;
        if (value) {
            probe = value->ToString();
        }
        auto definition = MakeStepStatisticsDefinition(
            NColumnShardStats::TOperation::kIndexCheck, TStringBuilder() << "Check:" << order << ':' << operation.GetSignalId() << ':' << probe);
        auto* details = definition.MutableIndexCheck();
        if (indexNode) {
            *details->MutableIndex() = indexNode->GetDefinition().GetIndexFilter().GetIndex();
        }
        details->SetApplicationOrder(order);
        details->SetCheckerType(CheckerType(operation));
        details->SetProbeValue(probe);
        details->SetCaseSensitive(operation.GetCaseSensitive());
        details->MutableIndexAvailable()->SetApplicability(NColumnShardStats::APPLICABLE);
        details->MutableDataAvailable()->SetApplicability(index ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
        details->MutableMatched()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        details->MutableMaskApplied()->SetApplicability(index ? NColumnShardStats::APPLICABLE : NColumnShardStats::NOT_APPLICABLE);
        details->MutableCandidates()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        details->MutableCandidates()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
        definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        definition.MutableStatistics()->SetInputRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
        result.Check =
            local->Measure(local->AddChild(*parent, definition), result.Phase ? result.Phase.GetHandle() : Source.GetCurrentStatisticsCall());
        result.Check.Observe([&](auto& observation) {
            if (index) {
                observation.Metric(&NColumnShardStats::TIndexCheckDetails::MutableIndexAvailable).SetTrueCount(1);
            } else {
                observation.Metric(&NColumnShardStats::TIndexCheckDetails::MutableIndexAvailable).SetFalseCount(1);
            }
        });
        result.InputRows = NPrivate::KnownRows(resources);
        if (!result.InputRows && resources.GetFilter().IsTotalAllowFilter()) {
            result.InputRows = Source.GetRecordsCount();
        }
        if (result.InputRows) {
            const auto observe = [&](auto& observation) {
                observation.Metric(&NColumnShardStats::TStatisticsDescription::MutableInputRows).SetValue(*result.InputRows);
            };
            result.Phase.Observe(observe);
            result.Check.Observe(observe);
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
