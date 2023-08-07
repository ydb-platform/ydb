#pragma once

#include <ydb/library/yql/core/yql_join.h>

#include <ydb/library/yql/providers/yt/provider/yql_yt_provider.h>
#include <ydb/library/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

#include <util/generic/ptr.h>
#include <util/generic/vector.h>
#include <util/generic/ylimits.h>

namespace NYql {

using namespace NNodes;

struct TYtJoinNodeOp;

struct TYtJoinNode: public TRefCounted<TYtJoinNode, TSimpleCounter> {
    using TPtr = TIntrusivePtr<TYtJoinNode>;
    virtual ~TYtJoinNode() = default;

    TVector<TString> Scope;
    TConstraintSet Constraints;
};

struct TYtJoinNodeLeaf : TYtJoinNode {
    using TPtr = TIntrusivePtr<TYtJoinNodeLeaf>;

    TYtJoinNodeLeaf(TYtSection section, TMaybeNode<TCoLambda> premap)
            : Section(section)
            , Premap(premap)
    {
    }

    TYtSection Section;
    TMaybeNode<TCoLambda> Premap;
    TExprNode::TPtr Label;
    size_t Index = Max<size_t>();
};

struct TYtStarJoinOption {
    TSet<TString> StarKeys;
    TVector<TString> StarSortedKeys;
    size_t StarInputIndex = Max<size_t>();
    TString StarLabel;
};

struct TYtJoinNodeOp : TYtJoinNode {
    using TPtr = TIntrusivePtr<TYtJoinNodeOp>;

    TYtJoinNode::TPtr Left;
    TYtJoinNode::TPtr Right;
    TExprNode::TPtr JoinKind;
    TExprNode::TPtr LeftLabel;
    TExprNode::TPtr RightLabel;
    TEquiJoinLinkSettings LinkSettings;
    const TYtJoinNodeOp* Parent = nullptr;
    TVector<TYtStarJoinOption> StarOptions;
    TMaybeNode<TYtOutputOpBase> Output;
    THashSet<TString> OutputRemoveColumns;
};

TYtJoinNodeOp::TPtr ImportYtEquiJoin(TYtEquiJoin equiJoin, TExprContext& ctx);
IGraphTransformer::TStatus RewriteYtEquiJoin(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx);
TMaybeNode<TExprBase> ExportYtEquiJoin(TYtEquiJoin equiJoin, const TYtJoinNodeOp& op, TExprContext& ctx, const TYtState::TPtr& state);
TYtJoinNodeOp::TPtr OrderJoins(TYtJoinNodeOp::TPtr op, const TYtState::TPtr& state, TExprContext& ctx, bool debug = false);

}
