#include "kqp_opt_phy_rules.h" 
#include "kqp_opt_phy_impl.h"
 
#include <ydb/core/kqp/common/kqp_yql.h>
 
namespace NKikimr::NKqp::NOpt {
 
using namespace NYql; 
using namespace NYql::NNodes; 
 
TExprBase KqpApplyLimitToReadTable(TExprBase node, TExprContext& ctx, const TKqpOptimizeContext& kqpCtx) { 
    if (!node.Maybe<TCoTake>()) { 
        return node; 
    } 
    auto take = node.Cast<TCoTake>(); 
 
    auto maybeSkip = take.Input().Maybe<TCoSkip>(); 
    auto input = maybeSkip ? maybeSkip.Cast().Input() : take.Input(); 
 
    bool isReadTable = input.Maybe<TKqpReadTable>().IsValid(); 
    bool isReadTableRanges = input.Maybe<TKqlReadTableRangesBase>().IsValid(); 
 
    if (!isReadTable && !isReadTableRanges) { 
        return node; 
    } 
 
    if (kqpCtx.IsScanQuery()) { 
        auto& tableDesc = kqpCtx.Tables->ExistingTable(kqpCtx.Cluster, GetReadTablePath(input, isReadTableRanges)); 
 
        if (tableDesc.Metadata->Kind != EKikimrTableKind::Olap) { 
            return node; 
        } 
    } 
 
    auto settings = GetReadTableSettings(input, isReadTableRanges); 
    if (settings.ItemsLimit) { 
        return node; // already set? 
    } 
 
    TMaybeNode<TExprBase> limitValue; 
    if (auto maybeTakeCount = take.Count().Maybe<TCoUint64>()) { 
        ui64 totalLimit; 
        ui64 takeValue = FromString<ui64>(maybeTakeCount.Cast().Literal().Value()); 
 
        if (maybeSkip) { 
            if (auto maybeSkipCount = maybeSkip.Count().Maybe<TCoUint64>()) { 
                auto skipValue = FromString<ui64>(maybeSkipCount.Cast().Literal().Value()); 
                totalLimit = takeValue + skipValue; 
            } else { 
                return node; // ??? 
            } 
        } else { 
            totalLimit = takeValue; 
        } 
 
        limitValue = Build<TCoUint64>(ctx, node.Pos()) 
            .Literal<TCoAtom>() 
            .Value(ToString(totalLimit)).Build() 
            .Done(); 
    } else { 
        limitValue = take.Count(); 
        if (maybeSkip) { 
            limitValue = Build<TCoPlus>(ctx, node.Pos()) 
                .Left(limitValue.Cast()) 
                .Right(maybeSkip.Cast().Count()) 
                .Done(); 
        } 
    } 
 
    YQL_CLOG(TRACE, ProviderKqp) << "-- set limit items value to " << limitValue.Cast().Ref().Dump(); 
 
    settings.SetItemsLimit(Build<TDqPrecompute>(ctx, node.Pos()) 
        .Input(limitValue.Cast()) 
        .Done().Ptr()); 
 
    input = BuildReadNode(node.Pos(), ctx, input, settings); 
 
    if (maybeSkip) { 
        input = Build<TCoSkip>(ctx, node.Pos()) 
            .Input(input) 
            .Count(maybeSkip.Cast().Count()) 
            .Done(); 
    } 
 
    return Build<TCoTake>(ctx, take.Pos()) 
        .Input(input) 
        .Count(take.Count()) 
        .Done(); 
} 
 
} // namespace NKikimr::NKqp::NOpt

