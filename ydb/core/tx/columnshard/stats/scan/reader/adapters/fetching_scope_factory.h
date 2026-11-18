#pragma once

#include "source_scope_factory.h"

#include <util/string/builder.h>

#include <algorithm>
#include <vector>

namespace NKikimr::NOlap::NStats::NScan {

template <class TSource, class TCursor>
class TFetchingScopeFactory {
    const TSource& Source;
    const TCursor& Cursor;

public:
    TFetchingScopeFactory(const TSource& source, const TCursor& cursor)
        : Source(source)
        , Cursor(cursor)
    {
    }

    template <class TColumns>
    std::shared_ptr<TOperationScope> StartColumns(const TColumns& columns) const {
        return TSourceScopeFactory(Source).StartStep(Cursor, NColumnShardStats::TOperation::kFetchingColumns, "Columns", [&](auto& definition) {
            definition.SetKey("Columns:" + columns.DebugString());
            if (const auto* resolver = Source.GetContext()->GetCommonContext()->GetResolver()) {
                for (const auto column : columns.GetColumnIds()) {
                    const auto name = resolver->GetColumnName(column, false);
                    if (name) {
                        definition.MutableFetchingColumns()->AddColumns(name);
                    }
                }
            }
            definition.MutableFetchingColumns()->MutableRequestedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
            definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
            definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
            definition.MutableStatistics()->SetInputRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
        });
    }

    template <class TFetchers>
    std::shared_ptr<TOperationScope> StartProviderColumns(const TFetchers& fetchers) const {
        return TSourceScopeFactory(Source).StartStep(
            Cursor, NColumnShardStats::TOperation::kFetchingColumns, "ProviderColumns", [&](auto& definition) {
                std::vector<ui32> entities;
                for (const auto& fetcher : fetchers) {
                    entities.push_back(fetcher->GetEntityId());
                }
                std::sort(entities.begin(), entities.end());
                TStringBuilder key;
                key << "ProviderColumns";
                for (const auto id : entities) {
                    key << ':' << id;
                    if (const auto* resolver = Source.GetContext()->GetCommonContext()->GetResolver()) {
                        const auto name = resolver->GetColumnName(id, false);
                        if (name) {
                            definition.MutableFetchingColumns()->AddColumns(name);
                        }
                    }
                }
                definition.SetKey(key);
                definition.MutableFetchingColumns()->MutableRequestedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
                definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                definition.MutableStatistics()->SetInputRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
            }, false);
    }

    template <class TFetchers>
    std::shared_ptr<TOperationScope> StartProviderIndexes(const TFetchers& fetchers) const {
        return TSourceScopeFactory(Source).StartStep(
            Cursor, NColumnShardStats::TOperation::kFetchingIndexes, "ProviderIndexes", [&](auto& definition) {
                std::vector<ui32> entities;
                for (const auto& fetcher : fetchers) {
                    entities.push_back(fetcher->GetEntityId());
                }
                std::sort(entities.begin(), entities.end());
                TStringBuilder key;
                key << "ProviderIndexes";
                for (const auto id : entities) {
                    key << ':' << id;
                    definition.MutableFetchingIndexes()->AddIndexIds(id);
                }
                definition.SetKey(key);
                definition.MutableFetchingIndexes()->MutableRequestedBytes()->SetApplicability(NColumnShardStats::APPLICABLE);
                definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                definition.MutableStatistics()->SetInputRepresentation(NColumnShardStats::ENCODED_PAYLOAD);
            }, false, true);
    }

    template <class TColumns>
    std::shared_ptr<TOperationScope> StartAssemble(const TColumns& columns) const {
        return TSourceScopeFactory(Source).StartStep(Cursor, NColumnShardStats::TOperation::kAssemble, "Columns", [&](auto& definition) {
            definition.SetKey("Columns:" + columns.DebugString());
            for (const auto& column : columns.GetColumnNamesVector()) {
                definition.MutableAssemble()->AddColumns(column);
            }
            definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
            definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        });
    }

    std::shared_ptr<TOperationScope> StartProviderAssemble(ui32 columnId) const {
        return TSourceScopeFactory(Source).StartStep(
            Cursor, NColumnShardStats::TOperation::kAssemble, "ProviderAccessor", [&](auto& definition) {
                definition.SetKey(TStringBuilder() << "ProviderAccessor:" << columnId);
                if (const auto* resolver = Source.GetContext()->GetCommonContext()->GetResolver()) {
                    const auto name = resolver->GetColumnName(columnId, false);
                    if (name) {
                        definition.MutableAssemble()->AddColumns(name);
                    }
                }
                definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
                definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::NOT_APPLICABLE);
            }, false);
    }

    template <class THeaderContext>
    std::shared_ptr<TOperationScope> StartHeaderFilter(const THeaderContext& header) const {
        return TSourceScopeFactory(Source).StartStep(Cursor, NColumnShardStats::TOperation::kFilter, "HeaderPresence", [&](auto& definition) {
            definition.SetKey(TStringBuilder() << "HeaderPresence:" << header.GetColumnId() << ':' << header.GetSubColumnName().GetValue());
            definition.MutableFilter()->SetKind(NColumnShardStats::HEADER_PRESENCE);
            definition.MutableFilter()->SetPredicate(header.GetSubColumnName().GetValue());
            definition.MutableStatistics()->MutableInputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
            definition.MutableStatistics()->MutableOutputRows()->SetApplicability(NColumnShardStats::APPLICABLE);
        }, false);
    }
};

}   // namespace NKikimr::NOlap::NStats::NScan
