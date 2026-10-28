#include "kqp_rbo_lookup_join.h"
#include "kqp_rbo_utils.h"

namespace NKikimr::NKqp {

using namespace NYql;
using namespace NYql::NNodes;

namespace {

bool IsValidIndex(const TIndexDescription& index) {
    return index.Type != TIndexDescription::EType::GlobalAsync
        && index.Type != TIndexDescription::EType::GlobalJson
        && index.Type != TIndexDescription::EType::GlobalJsonCompact
        && index.Type != TIndexDescription::EType::LocalMinMax
        && index.Type != TIndexDescription::EType::LocalBloomFilter
        && index.Type != TIndexDescription::EType::LocalBloomNgramFilter
        && index.State == TIndexDescription::EIndexState::Ready;
}

bool IsCoveringIndex(const TVector<TString>& readColumns, const TVector<TString>& keyColumns, const TVector<TString>& dataColumns) {
    THashSet<TString> indexColumnSet(keyColumns.begin(), keyColumns.end());
    indexColumnSet.insert(dataColumns.begin(), dataColumns.end());
    for (const auto& column : readColumns) {
        if (!indexColumnSet.contains(column)) {
            return false;
        }
    }
    return true;
}

TIntrusivePtr<TKikimrTableMetadata> TryToFindBestIndexForRightSide(const TKikimrTableDescription& mainTableDesc,
                                                                   const TVector<TString>& readColumns,
                                                                   const THashSet<TString>& rightJoinKeys) {
    const auto& meta = *mainTableDesc.Metadata;
    std::optional<TString> bestIndexName;
    ui32 bestPrefix = 0;

    for (const auto& index : meta.Indexes) {
        if (!IsValidIndex(index) || !IsCoveringIndex(readColumns, index.KeyColumns, index.DataColumns)) {
            continue;
        }

        ui32 currentPrefix = 0;
        for (const auto& keyCol : index.KeyColumns) {
            if (!rightJoinKeys.contains(keyCol)) {
                break;
            }
            ++currentPrefix;
        }

        // Better prefix wins and ties broken alphabetically by index name.
        if (currentPrefix > bestPrefix || (currentPrefix == bestPrefix && currentPrefix > 0 && index.Name < *bestIndexName)) {
            bestPrefix = currentPrefix;
            bestIndexName = index.Name;
        }
    }

    if (bestIndexName.has_value()) {
        return meta.GetIndexMetadata(*bestIndexName).first;
    }

    return nullptr;
}

bool IsLookupTable(const TKikimrTableMetadata& meta) {
    // Can't lookup in system views: a read of one is not a datashard read even though it is
    // described as a row storage read.
    return meta.SysView.empty() && meta.Kind != EKikimrTableKind::SysView && !meta.KeyColumnNames.empty();
}

bool IsUsablePointPrefix(const TOpRead::TPointPrefix& prefix, const TVector<TString>& keyColumnNames, bool innerJoin,
                         size_t pointsLimit) {
    if (!prefix.Points || !prefix.PointsItemType || prefix.Columns.empty()) {
        return false;
    }

    if (prefix.Columns.size() >= keyColumnNames.size()) {
        return false;
    }

    for (size_t i = 0; i < prefix.Columns.size(); ++i) {
        if (prefix.Columns[i] != keyColumnNames[i]) {
            return false;
        }
    }

    // For left, left only, left semi joins we cannot support more than 1 point lookup.
    if (!innerJoin) {
        pointsLimit = std::min<size_t>(pointsLimit, 1);
    }

    return prefix.ExpectedMaxPoints.Defined() && *prefix.ExpectedMaxPoints <= pointsLimit;
}

bool IsCoveringTable(const TKikimrTableMetadata& meta, const TVector<TString>& columns) {
    return std::all_of(columns.begin(), columns.end(), [&](const TString& column) { return meta.Columns.contains(column); });
}

// Rows found in a secondary index of the main table can be fetched from the main table by its primary key.
bool IsSecondaryIndexOf(const TKikimrTableMetadata& mainMeta, const TString& indexTable) {
    for (size_t i = 0; i < mainMeta.Indexes.size() && i < mainMeta.ImplTables.size(); ++i) {
        const auto& index = mainMeta.Indexes[i];
        const auto& implTable = mainMeta.ImplTables[i];
        if (implTable && implTable->Name == indexTable) {
            return (index.Type == TIndexDescription::EType::GlobalSync || index.Type == TIndexDescription::EType::GlobalSyncUnique)
                && index.State == TIndexDescription::EIndexState::Ready;
        }
    }
    return false;
}

// Returns how many leading key columns a lookup fixes: the point prefix and the join keys after it.
size_t GetLookupKeyPrefixLen(const TVector<TString>& keyColumnNames, size_t pointPrefixLen, const THashSet<TString>& joinKeyColumns) {
    size_t len = pointPrefixLen;
    while (len < keyColumnNames.size() && joinKeyColumns.contains(keyColumnNames[len])) {
        ++len;
    }
    return len;
}

} // anonymous namespace

std::optional<TLookupJoinRightSide> MatchLookupJoinRightSide(const TIntrusivePtr<IOperator>& input) {
    auto op = input;
    TIntrusivePtr<TOpTableLookup> sourceLookup;
    if (op->Kind == EOperator::TableLookup) {
        sourceLookup = CastOperator<TOpTableLookup>(op);
        if (sourceLookup->IsJoin() || !sourceLookup->SourceRead || sourceLookup->Parents.size() > 1) {
            return std::nullopt;
        }
        op = sourceLookup->GetInput();
    }

    TIntrusivePtr<TOpFilter> filter;
    if (op->Kind == EOperator::Filter) {
        if (op->Parents.size() > 1) {
            return std::nullopt;
        }
        filter = CastOperator<TOpFilter>(op);
        op = filter->GetInput();
    }

    if (op->Kind != EOperator::Source || op->Parents.size() > 1) {
        return std::nullopt;
    }

    auto read = CastOperator<TOpRead>(op);
    if (!sourceLookup) {
        return TLookupJoinRightSide{read, filter};
    }

    // The lookup join replaces the whole subtree, so the index read and the filter above it are not needed:
    // the source read predicate contains the whole read predicate.
    const auto& source = *sourceLookup->SourceRead;
    TOpRead::TRangeInfo rangeInfo;
    rangeInfo.PointPrefixes = source.PointPrefixes;
    auto sourceRead = MakeIntrusive<TOpRead>(read->Alias, sourceLookup->GetColumns(), NYql::EStorageType::RowStorage,
                                             sourceLookup->Table, nullptr, nullptr, std::move(rangeInfo), source.Predicate,
                                             ESortDir::None, sourceLookup->Props, sourceLookup->Pos);
    sourceRead->Type = sourceLookup->Type;
    return TLookupJoinRightSide{sourceRead, nullptr};
}

std::optional<THashSet<TString>> GetLookupJoinKeyColumns(const TOpRead& read, const TUnorderedIUs& rightJoinKeys,
                                                         const TInfoUnitRegistry& registry) {
    const auto& readColumns = read.GetColumns();
    THashSet<TString> columns;
    for (const auto key : rightJoinKeys) {
        if (!readColumns.Contains(key)) {
            return std::nullopt;
        }
        columns.insert(registry.Get(key).GetColumnName());
    }
    return columns;
}

std::optional<TLookupJoinTarget> ChooseLookupJoinTarget(const TOpRead& read, const TVector<TString>& readColumns,
                                                        const THashSet<TString>& joinKeyColumns, bool innerJoin,
                                                        const NOpt::TKqpOptimizeContext& kqpCtx) {
    // Only supports row storage tables.
    if (read.GetTableStorageType() != NYql::EStorageType::RowStorage) {
        return std::nullopt;
    }

    const auto table = TKqpTable(read.GetTable());
    if (table.PathId().Value().empty()) {
        return std::nullopt;
    }

    const auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, table.Path().Value());
    Y_ENSURE(tableDesc.Metadata);
    // A read redirected to an index is not on the main table, but rows found in another index of the main table
    // are still fetched from the main table.
    const bool redirected = read.RangeInfo.has_value() && !read.RangeInfo->MainTable.empty()
        && read.RangeInfo->MainTable != table.Path().Value();
    const auto& mainTableDesc = redirected ? kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, read.RangeInfo->MainTable) : tableDesc;
    Y_ENSURE(mainTableDesc.Metadata);
    const bool autoIndexSelection = kqpCtx.Config->IsAutoIndexSelectionForIndexLookupJoinEnabled();
    const bool allPointPrefixes = kqpCtx.Config->GetEnableLookupJoinPointPrefixes();

    // Without a pushed predicate the read can be redirected to a covering index whose key starts with join keys.
    if (autoIndexSelection && !read.RangeInfo.has_value()) {
        if (auto index = TryToFindBestIndexForRightSide(tableDesc, readColumns, joinKeyColumns); index && IsLookupTable(*index)) {
            return TLookupJoinTarget{index, nullptr, nullptr};
        }
    }

    const size_t pointsLimit = kqpCtx.Config->GetIdxLookupJoinPointsLimit();
    std::optional<TLookupJoinTarget> best;
    // The longest looked up key prefix wins, and a covering table wins a tie, because it needs a single lookup.
    std::pair<size_t, bool> bestScore;
    // Candidates are considered in the order of preference, so the first one wins a tie.
    auto consider = [&](const TIntrusivePtr<TKikimrTableMetadata>& meta, const TOpRead::TPointPrefix* prefix) {
        if (!IsLookupTable(*meta)) {
            return;
        }
        if (prefix && !IsUsablePointPrefix(*prefix, meta->KeyColumnNames, innerJoin, pointsLimit)) {
            return;
        }

        TIntrusivePtr<TKikimrTableMetadata> mainTable;
        if (!IsCoveringTable(*meta, readColumns)) {
            // Rows found in a non-covering index are fetched from the main table by a second lookup. Only an inner join
            // can drop the rows without a match before the second lookup, other joins would need its result for them.
            if (!innerJoin || !IsLookupTable(*mainTableDesc.Metadata) || !IsSecondaryIndexOf(*mainTableDesc.Metadata, meta->Name)) {
                return;
            }
            mainTable = mainTableDesc.Metadata;
        }

        const size_t pointPrefixLen = prefix ? prefix->Columns.size() : 0;
        const size_t len = GetLookupKeyPrefixLen(meta->KeyColumnNames, pointPrefixLen, joinKeyColumns);
        // At least one join key is needed to lookup by.
        if (len == pointPrefixLen) {
            return;
        }

        const std::pair<size_t, bool> score{len, !mainTable};
        if (best && score <= bestScore) {
            return;
        }

        best = TLookupJoinTarget{meta, prefix, mainTable};
        bestScore = score;
    };

    // The read table is preferred, so the read is not redirected without a reason.
    TVector<const TOpRead::TPointPrefix*> otherPrefixes;
    if (read.RangeInfo.has_value()) {
        for (const auto& prefix : read.RangeInfo->PointPrefixes) {
            if (prefix.Table == table.Path().Value()) {
                consider(tableDesc.Metadata, &prefix);
            } else if (autoIndexSelection && allPointPrefixes) {
                otherPrefixes.push_back(&prefix);
            }
        }
    }
    consider(tableDesc.Metadata, nullptr);
    // A read redirected to an index can be probed in the main table instead, which covers every column. It gets
    // no point prefix above unless the predicate pins its key.
    if (redirected && autoIndexSelection && allPointPrefixes) {
        consider(mainTableDesc.Metadata, nullptr);
    }

    // Ties are broken deterministically, independently of the order indexes are declared.
    std::sort(otherPrefixes.begin(), otherPrefixes.end(), [](const auto* lhs, const auto* rhs) { return lhs->Table < rhs->Table; });
    for (const auto* prefix : otherPrefixes) {
        const auto& otherTableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, prefix->Table);
        Y_ENSURE(otherTableDesc.Metadata);
        consider(otherTableDesc.Metadata, prefix);
    }

    return best;
}

TExprNode::TPtr BuildTableCallable(const TKikimrTableMetadata& meta, TPositionHandle pos, TExprContext& ctx) {
    // clang-format off
    return Build<TKqpTable>(ctx, pos)
        .Path().Build(meta.Name)
        .PathId().Build(meta.PathId.ToString())
        .SysView().Build(meta.SysView)
        .Version().Build(meta.SchemaVersion)
    .Done().Ptr();
    // clang-format on
}

} // namespace NKikimr::NKqp
