#include "kqp_opt_cbo.h"
#include "kqp_opt_log_impl.h"

#include <ydb/library/yql/core/yql_opt_utils.h>
#include <ydb/library/yql/utils/log/log.h>


namespace NKikimr::NKqp::NOpt {

using namespace NYql;
using namespace NYql::NCommon;
using namespace NYql::NDq;
using namespace NYql::NNodes;

namespace {

TMaybeNode<TKqlKeyInc> GetRightTableKeyPrefix(const TKqlKeyRange& range) {
    if (!range.From().Maybe<TKqlKeyInc>() || !range.To().Maybe<TKqlKeyInc>()) {
        return {};
    }
    auto rangeFrom = range.From().Cast<TKqlKeyInc>();
    auto rangeTo = range.To().Cast<TKqlKeyInc>();

    if (rangeFrom.ArgCount() != rangeTo.ArgCount()) {
        return {};
    }
    for (ui32 i = 0; i < rangeFrom.ArgCount(); ++i) {
        if (rangeFrom.Arg(i).Raw() != rangeTo.Arg(i).Raw()) {
            return {};
        }
    }

    return rangeFrom;
}

/**
 * KQP specific rule to check if a LookupJoin is applicable
*/
bool IsLookupJoinApplicableDetailed(const std::shared_ptr<NYql::TRelOptimizerNode>& node, const TVector<TString>& joinColumns, const TKqpProviderContext& ctx) {

    auto rel = std::static_pointer_cast<TKqpRelOptimizerNode>(node);
    auto expr = TExprBase(rel->Node);

    if (ctx.KqpCtx.IsScanQuery() && !ctx.KqpCtx.Config->EnableKqpScanQueryStreamIdxLookupJoin) {
        return false;
    }

    if (find_if(joinColumns.begin(), joinColumns.end(), [&] (const TString& s) { return node->Stats->KeyColumns[0] == s;})) {
        return true;
    }

    auto readMatch = MatchRead<TKqlReadTable>(expr);
    TMaybeNode<TKqlKeyInc> maybeTablePrefix;
    size_t prefixSize;

    if (readMatch) {
        if (readMatch->FlatMap && !IsPassthroughFlatMap(readMatch->FlatMap.Cast(), nullptr)){
            return false;
        }
        auto read = readMatch->Read.Cast<TKqlReadTable>();
        maybeTablePrefix = GetRightTableKeyPrefix(read.Range());

        if (!maybeTablePrefix) {
            return false;
        }

         prefixSize = maybeTablePrefix.Cast().ArgCount();

        if (!prefixSize) {
            return true;
        }
    } 
    else {
        readMatch = MatchRead<TKqlReadTableRangesBase>(expr);
        if (readMatch) {
            if (readMatch->FlatMap && !IsPassthroughFlatMap(readMatch->FlatMap.Cast(), nullptr)){
                return false;
            }
            auto read = readMatch->Read.Cast<TKqlReadTableRangesBase>();
            if (TCoVoid::Match(read.Ranges().Raw())) {
                return true;
            } else {
                auto prompt = TKqpReadTableExplainPrompt::Parse(read);

                if (prompt.PointPrefixLen != prompt.UsedKeyColumns.size()) {
                    return false;
                }

                if (prompt.ExpectedMaxRanges != TMaybe<ui64>(1)) {
                    return false;
                }
                prefixSize = prompt.PointPrefixLen;
            }
        }
    }
    if (! readMatch) {
        return false;
    }

    if (prefixSize < node->Stats->KeyColumns.size() && !(find_if(joinColumns.begin(), joinColumns.end(), [&] (const TString& s) {
            return node->Stats->KeyColumns[prefixSize] == s;
        }))){
            return false;
        }

    return true;
}

bool IsLookupJoinApplicable(std::shared_ptr<IBaseOptimizerNode> left, 
    std::shared_ptr<IBaseOptimizerNode> right, 
    const std::set<std::pair<TJoinColumn, TJoinColumn>>& joinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    TKqpProviderContext& ctx) {

    Y_UNUSED(left);
    Y_UNUSED(leftJoinKeys);

    auto rightStats = right->Stats;

    if (rightStats->Type != EStatisticsType::BaseTable) {
        return false;
    }
    if (joinConditions.size() > rightStats->KeyColumns.size()) {
        return false;
    }

    for (auto [leftCol, rightCol] : joinConditions) {
        // Fix for clang14, somehow structured binding does not create a variable in clang14
        auto r = rightCol;
        if (! find_if(rightStats->KeyColumns.begin(), rightStats->KeyColumns.end(), 
            [r] (const TString& s) {
            return r.AttributeName == s;
        } )) {
            return false;
        }
    }

    return IsLookupJoinApplicableDetailed(std::static_pointer_cast<TRelOptimizerNode>(right), rightJoinKeys, ctx);
}

}

bool TKqpProviderContext::IsJoinApplicable(const std::shared_ptr<IBaseOptimizerNode>& left, 
    const std::shared_ptr<IBaseOptimizerNode>& right, 
    const std::set<std::pair<NDq::TJoinColumn, NDq::TJoinColumn>>& joinConditions,
    const TVector<TString>& leftJoinKeys,
    const TVector<TString>& rightJoinKeys,
    EJoinAlgoType joinAlgo)  {

    switch( joinAlgo ) {
        case EJoinAlgoType::LookupJoin:
            if ((OptLevel >= 2) && (left->Stats->Nrows > 1000)) {
                return false;
            }
            return IsLookupJoinApplicable(left, right, joinConditions, leftJoinKeys, rightJoinKeys, *this);

        case EJoinAlgoType::MapJoin:
            return right->Stats->ByteSize < 5e8;
        case EJoinAlgoType::GraceJoin:
            return true;
        default:
            return false;
    }
}

double TKqpProviderContext::ComputeJoinCost(const TOptimizerStatistics& leftStats, const TOptimizerStatistics& rightStats, const double outputRows, const double outputByteSize, EJoinAlgoType joinAlgo) const  {
    Y_UNUSED(outputByteSize);
    
    switch(joinAlgo) {
        case EJoinAlgoType::LookupJoin:
            if (OptLevel==1) {
                return -1;
            }
            return leftStats.Nrows + outputRows;
        case EJoinAlgoType::MapJoin:
            return leftStats.Nrows + 1.8 * rightStats.Nrows + outputRows;
        case EJoinAlgoType::GraceJoin:
            return leftStats.Nrows + 2.0 * rightStats.Nrows + outputRows;
        default:
            Y_ENSURE(false, "Illegal join type encountered");
            return 0;
    }
}


}
