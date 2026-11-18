#pragma once

#include "source_rows_observer.h"

#include <ydb/core/tx/columnshard/stats/scan/observation.h>
#include <ydb/core/tx/columnshard/stats/scan/reader/operations.h>

#include <memory>
#include <optional>
#include <type_traits>
#include <utility>

namespace NKikimr::NOlap::NStats::NScan {

template <class TSource>
class TSourceScopeFactory {
    const TSource& Source;

public:
    explicit TSourceScopeFactory(const TSource& source)
        : Source(source)
    {
    }

    template <class TFactory>
    TOperationScope StartOperation(NColumnShardStats::TOperation::DetailsCase group, TFactory&& factory) const {
        auto* local = Source.GetLocalStats();
        if (!local) {
            return {};
        }
        auto* parent = &local->GetRoot();
        if (group != NColumnShardStats::TOperation::kReader) {
            parent = local->AddChild(*parent, MakeMetastepStatisticsDefinition(group));
        }
        if (!parent) {
            return {};
        }
        auto definition = std::forward<TFactory>(factory)();
        auto scope = local->Measure(local->AddChild(*parent, definition), Source.GetCurrentStatisticsCall());
        if (scope) {
            Source.SetActiveStatistics(scope.GetHandle());
        }
        return scope;
    }

    template <class TDescribe>
    TOperationScope StartOperation(NColumnShardStats::TOperation::DetailsCase group, NColumnShardStats::TOperation::DetailsCase kind,
        TStringBuf key, TDescribe&& describe) const {
        return StartOperation(group, [&] {
            auto definition = MakeStepStatisticsDefinition(kind, key);
            std::forward<TDescribe>(describe)(definition);
            return definition;
        });
    }

    template <class TCursor, class TFactory>
        requires std::is_invocable_r_v<NColumnShardStats::TOperation, TFactory>
    std::shared_ptr<TOperationScope> StartStep(
        const TCursor& cursor, TFactory&& factory, bool attachToCursor = true, bool forceReaderParent = false) const {
        auto* local = Source.GetLocalStats();
        if (!local) {
            return {};
        }
        const auto& observer = cursor.GetStatistics();
        if (attachToCursor && observer.GetStepStatistics() && *observer.GetStepStatistics()) {
            observer.GetStepStatistics()->Resume();
            Source.SetActiveStatistics(observer.GetStepStatistics()->GetHandle());
            return observer.GetStepStatistics();
        }
        auto* parent = &local->GetRoot();
        if (const auto& program = Source.GetExecutionContext().GetProgramStats(); !forceReaderParent && program && program->GetCurrentNode()) {
            parent = local->CloneBranch(*program->GetCurrentNode());
        } else if (!forceReaderParent && observer.GetMetastepStatistics() && *observer.GetMetastepStatistics()) {
            parent = observer.GetMetastepStatistics()->GetNode();
        }
        if (!parent) {
            return {};
        }
        auto definition = std::forward<TFactory>(factory)();
        auto scope = std::make_shared<TOperationScope>();
        *scope = local->Measure(local->AddChild(*parent, definition), Source.GetCurrentStatisticsCall());
        if (*scope) {
            Source.SetActiveStatistics(scope->GetHandle());
        }
        if (attachToCursor) {
            observer.SetStepStatistics(scope);
        }
        return scope;
    }

    template <class TCursor, class TDescribe>
    std::shared_ptr<TOperationScope> StartStep(const TCursor& cursor, NColumnShardStats::TOperation::DetailsCase kind, TStringBuf key,
        TDescribe&& describe, bool attachToCursor = true, bool forceReaderParent = false) const {
        return StartStep(cursor, [&] {
            auto definition = MakeStepStatisticsDefinition(kind, key);
            std::forward<TDescribe>(describe)(definition);
            return definition;
        }, attachToCursor, forceReaderParent);
    }

    TOperationScope StartFilter(NColumnShardStats::EFilterKind kind) const {
        auto scope = StartOperation(NColumnShardStats::TOperation::kPKFilter, NColumnShardStats::TOperation::kFilter,
            NColumnShardStats::EFilterKind_Name(kind), [&](auto& definition) {
                definition.MutableFilter()->SetKind(kind);
                definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
                definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
            });
        TSourceRowsObserver(Source).OnInputRows(scope);
        return scope;
    }

    TOperationScope StartBuildResult() const {
        return StartOperation(
            NColumnShardStats::TOperation::kBuildResult, NColumnShardStats::TOperation::kPrepareResult, "StageResult", [](auto& definition) {
                definition.MutablePrepareResult()->SetAction(NColumnShardStats::BUILD_STAGE_RESULT);
                definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
                definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
            });
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
