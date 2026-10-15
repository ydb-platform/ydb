#include "kqp_rbo_cbo.h"
#include "kqp_rbo_lookup_join.h"
#include "kqp_rbo_utils.h"

namespace {

using namespace NYql;
using namespace NYql::NNodes;
using namespace NKikimr::NKqp;

std::optional<TExpression> BuildFetchedRowFilter(const TOpRead& read, TOpFilter* filter, bool& supported) {
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
    for (const auto& conjunct : conjuncts) {
        if (!conjunct.GetInputIUs(/*includeSubplanVars=*/true, /*includeCorrelatedDeps=*/true).IsSubsetOf(read.GetColumns())) {
            supported = false;
            return std::nullopt;
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

bool IsUsablePointPrefix(const TOpRead::TPointPrefix& prefix, const TVector<TString>& keyColumnNames, EJoinKind joinKind,
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
    if (joinKind != EJoinKind::InnerJoin) {
        pointsLimit = std::min<size_t>(pointsLimit, 1);
    }

    return prefix.ExpectedMaxPoints.Defined() && *prefix.ExpectedMaxPoints <= pointsLimit;
}

// A shared producer is reached through a Replicate port.
bool IsSharedRelNode(const std::shared_ptr<IBaseOptimizerNode>& node) {
    if (node->Kind != EOptimizerNodeKind::RelNodeType) {
        return false;
    }
    const auto& op = std::static_pointer_cast<TRBORelOptimizerNode>(node)->Op;
    return op->Kind == EOperator::Replicate;
}

bool IsLookupJoinApplicableDetailed(const std::shared_ptr<TRelOptimizerNode>& node, const TVector<TJoinColumn>& joinColumns, EJoinKind joinKind, const TKqpProviderContext& ctx,
    const TColumnLineage& lineage) {
    auto rel = std::static_pointer_cast<TRBORelOptimizerNode>(node);
    // The rewrite rule has to be able to rewrite every lookup join chosen here, so both match the right
    // side the same way, including a read already redirected to a non-covering index.
    const auto rightSide = MatchLookupJoinRightSide(rel->Op);
    if (!rightSide) {
        return false;
    }
    auto* read = rightSide->Read.Get();
    auto* rightFilter = rightSide->Filter.Get();
    Y_ENSURE(read->Props.Metadata, "Lookup applicability requires Read metadata");
    THashSet<TString> rightJoinKeys;
    for (const auto& joinCol : joinColumns) {
        const auto id = GetCBOColumnId(joinCol);
        if (!rel->Op->GetOutputIUs().Contains(id)) {
            return false;
        }
        const auto* source = lineage.Find(id);
        if (!source) {
            return false;
        }
        rightJoinKeys.insert(source->ColumnName);
    }

    TVector<TString> columns;
    columns.reserve(read->GetColumns().Size());
    for (const auto id : read->GetColumns()) {
        const auto* source = lineage.Find(id);
        Y_ENSURE(source, "Read column without lineage");
        columns.push_back(source->ColumnName);
    }

    // The rewrite rule has to be able to rewrite every lookup join chosen here, so both use one chooser.
    const auto target = ChooseLookupJoinTarget(*read, columns, rightJoinKeys, joinKind == EJoinKind::InnerJoin, ctx.KqpCtx);
    if (target) {
        return true;
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
    if (read->RangeInfo) {
        if (const auto* prefix = read->RangeInfo->FindPointPrefix(tableMeta->Name);
            prefix && IsUsablePointPrefix(*prefix, tableMeta->KeyColumnNames, joinKind, ctx.KqpCtx.Config->GetIdxLookupJoinPointsLimit())) {
            pointPrefixLen = prefix->Columns.size();
        }
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
    TKqpProviderContext& ctx,
    const TColumnLineage& lineage
) {
    Y_UNUSED(leftJoinKeys);

    // We need to follow rewrite rule.
    if (IsSharedRelNode(left)) {
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

    return IsLookupJoinApplicableDetailed(std::static_pointer_cast<TRelOptimizerNode>(right), rightJoinKeys, joinKind, ctx, lineage);
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
            return IsLookupJoinApplicable(left, right, leftJoinKeys, rightJoinKeys, joinKind, *this, Lineage);
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
            return IsLookupJoinApplicable(right, left, rightJoinKeys, leftJoinKeys, joinKind, *this, Lineage);
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
