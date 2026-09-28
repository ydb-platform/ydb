#include "kqp_rbo_cbo.h"
#include "kqp_rbo_utils.h"

namespace {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;

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

TIntrusivePtr<TKikimrTableMetadata> TryToFindBestIndexForRightSide(const TKikimrTableDescription& mainTableDesc, const TVector<TString>& readColumns,
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

std::optional<TExpression> BuildFetchedRowFilter(const TOpRead& read, const TIntrusivePtr<TOpFilter>& filter, bool& supported) {
    TVector<TExpression> conjuncts;
    if (read.RangeInfo.has_value()) {
        if (!read.OriginalPredicate.has_value()) {
            supported = false;
            return std::nullopt;
        }
        const auto original = read.OriginalPredicate->SplitConjunct();
        conjuncts.insert(conjuncts.end(), original.begin(), original.end());
    }
    if (filter) {
        const auto filters = filter->GetFilterExpression().SplitConjunct();
        conjuncts.insert(conjuncts.end(), filters.begin(), filters.end());
    }

    if (conjuncts.empty()) {
        return std::nullopt;
    }

    // The filter is evaluated on a fetched row, so it can only refer to the fetched columns.
    const auto readOutputs = MakeInfoUnitSet(read.OutputIUs);
    for (const auto& conjunct : conjuncts) {
        for (const auto& iu : conjunct.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true)) {
            if (!readOutputs.contains(iu)) {
                supported = false;
                return std::nullopt;
            }
        }
    }

    return MakeConjunction(conjuncts);
}

struct TLookupKey {
    TInfoUnit LeftIU;
    TInfoUnit RightIU;
    TString Column;
};

struct TKeyMatch {
    // Represents a constant prefix keys.
    TVector<TLookupKey> PrefixKeys;
    // Represents a lookup keys.
    TVector<TLookupKey> LookupKeys;
    // Represents a join keys which are not present in the right side index.
    TVector<TLookupKey> ResidualKeys;
};

bool MatchKeyPrefix(const THashSet<TString>& joinKeys, const TVector<TString>& keyColumnNames,
                                        size_t pointPrefixLen) {
    Y_ENSURE(pointPrefixLen < keyColumnNames.size());

    auto firstKeyColumn = keyColumnNames[pointPrefixLen];
    return joinKeys.contains(firstKeyColumn);
}

bool IsUsablePointPrefix(const TOpRead::TRangeInfo& ranges, const TVector<TString>& keyColumnNames, EJoinKind joinKind,
                         size_t pointsLimit) {
    if (!ranges.Points || !ranges.PointsItemType || ranges.PointColumns.empty()) {
        return false;
    }

    if (ranges.PointColumns.size() >= keyColumnNames.size()) {
        return false;
    }

    for (size_t i = 0; i < ranges.PointColumns.size(); ++i) {
        if (ranges.PointColumns[i] != keyColumnNames[i]) {
            return false;
        }
    }

    // For left, left only, left semi joins we cannot support more than 1 point lookup.
    if (joinKind != EJoinKind::InnerJoin) {
        pointsLimit = std::min<size_t>(pointsLimit, 1);
    }

    return ranges.ExpectedMaxPoints.Defined() && *ranges.ExpectedMaxPoints <= pointsLimit;
}

// Checks whether a node is single consumer.
bool IsSingleConsumerRelNode(const std::shared_ptr<IBaseOptimizerNode>& node) {
    if (node->Kind != EOptimizerNodeKind::RelNodeType) {
        return true;
    }
    const auto& op = std::static_pointer_cast<TRBORelOptimizerNode>(node)->Op;
    return op->IsSingleConsumer();
}

bool IsLookupJoinApplicableDetailed(const std::shared_ptr<TRelOptimizerNode>& node, const TVector<TJoinColumn>& joinColumns, EJoinKind joinKind, const TKqpProviderContext& ctx) {
    auto rel = std::static_pointer_cast<TRBORelOptimizerNode>(node);
    auto rightInput = rel->Op;
    TIntrusivePtr<TOpFilter> rightFilter;

    if (rightInput->Kind == EOperator::Filter) {
        if (!rightInput->IsSingleConsumer()) {
            return false;
        }
        rightFilter = CastOperator<TOpFilter>(rightInput);
        rightInput = rightFilter->GetInput();
    }
    if (rightInput->Kind != EOperator::Source || !rightInput->IsSingleConsumer()) {
        return false;
    }

    THashSet<TString> rightJoinKeys;
    for (const auto& joinCol : joinColumns) {
        TInfoUnit joinIU(joinCol.RelName, joinCol.AttributeName);
        if (!rel->CBOToColumns.contains(joinIU)) {
            return false;
        }
        auto originalIU = rel->CBOToColumns.at(joinIU);
        rightJoinKeys.insert(originalIU.GetColumnName());
    }

    auto read = CastOperator<TOpRead>(rightInput);
    if (ctx.KqpCtx.Config->IsAutoIndexSelectionForIndexLookupJoinEnabled()) {
        // We cannot change the right side, if predicate was pushed.
        if (!read->RangeInfo.has_value()) {
            const auto table = TKqpTable(read->GetTable());
            const auto& mainTableDesc = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, table.Path().Value());

            if (auto index = TryToFindBestIndexForRightSide(mainTableDesc, read->Columns, rightJoinKeys)) {
                return true;
            }
        }
    }

    bool filterSupported = true;
    const auto fetchedRowFilter = BuildFetchedRowFilter(*read, rightFilter, filterSupported);
    if (!filterSupported) {
        return false;
    }

    const auto table = TKqpTable(read->GetTable());
    if (table.PathId().Value().empty()) {
        return false;
    }

    const auto& tableMeta = ctx.KqpCtx.Tables->ExistingTable(ctx.KqpCtx.Cluster, table.Path().Value()).Metadata;
    Y_ENSURE(tableMeta);
    if (!table.SysView().Value().empty() || tableMeta->Kind == EKikimrTableKind::SysView) {
        // Can't lookup in system views: a read of one is not a datashard read even though it is
        // described as a row storage read.
        return false;
    }

    if (tableMeta->KeyColumnNames.empty()) {
        return false;
    }

    size_t pointPrefixLen = 0;
    if (read->RangeInfo && IsUsablePointPrefix(*read->RangeInfo, tableMeta->KeyColumnNames, joinKind, ctx.KqpCtx.Config->GetIdxLookupJoinPointsLimit())) {
        pointPrefixLen = read->RangeInfo->PointColumns.size();
    }

    if (MatchKeyPrefix(rightJoinKeys, tableMeta->KeyColumnNames, pointPrefixLen)) {
        return true;
    } else {
        return MatchKeyPrefix(rightJoinKeys, tableMeta->KeyColumnNames, 0);
    }
}

bool IsLookupJoinApplicable(std::shared_ptr<IBaseOptimizerNode> left,
    std::shared_ptr<IBaseOptimizerNode> right,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinKind joinKind,
    TKqpProviderContext& ctx
) {
    Y_UNUSED(leftJoinKeys);

    // We need to follow rewrite rule.
    if (!IsSingleConsumerRelNode(left)) {
        return false;
    }

    if (!(right->Stats.StorageType == NKikimr::NKqp::EStorageType::RowStorage)) {
        return false;
    }

    auto rightStats = right->Stats;

    if (!rightStats.KeyColumns) {
        return false;
    }

    if (rightStats.Type != NKikimr::NKqp::EStatisticsType::BaseTable) {
        return false;
    }

    // for (auto rightCol : rightJoinKeys) {
    //     if (find(rightStats.KeyColumns->Data.begin(), rightStats.KeyColumns->Data.end(), rightCol.AttributeName) == rightStats.KeyColumns->Data.end()) {
    //         return false;
    //     }
    // }

    return IsLookupJoinApplicableDetailed(std::static_pointer_cast<TRelOptimizerNode>(right), rightJoinKeys, joinKind, ctx);
}

}

namespace NKikimr::NKqp::NOpt {

bool TRBOProviderContext::IsJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& left,
    const std::shared_ptr<IBaseOptimizerNode>& right,
    const TVector<TJoinColumn>& leftJoinKeys,
    const TVector<TJoinColumn>& rightJoinKeys,
    EJoinAlgoType joinAlgo,
    EJoinKind joinKind) {

    switch( joinAlgo ) {
        case EJoinAlgoType::LookupJoin: {
            if ((OptLevel != 3) && (left->Stats.Nrows > 5000)) {
                return false;
            }
            return IsLookupJoinApplicable(left, right, leftJoinKeys, rightJoinKeys, joinKind, *this);
        }
        // FIXME: Don't pick reverse lookup join yet
        /*
        case EJoinAlgoType::LookupJoinReverse: {
            if (joinKind != EJoinKind::LeftSemi) {
                return false;
            }
            if ((OptLevel != 3) && (right->Stats.Nrows > 5000)) {
                return false;
            }
            return IsLookupJoinApplicable(right, left, rightJoinKeys, leftJoinKeys, joinKind, *this);
        }
        */
        case EJoinAlgoType::MapJoin:
            return joinKind != EJoinKind::OuterJoin && joinKind != EJoinKind::Exclusion && right->Stats.ByteSize < 1e6;
        case EJoinAlgoType::GraceJoin:
            return true;
        case EJoinAlgoType::ReverseBlockJoin:
            return BlockJoinEnabled && (joinKind == EJoinKind::LeftJoin | joinKind == EJoinKind::LeftOnly | joinKind == EJoinKind::LeftSemi);
        default:
            return false;
    }
}
}
