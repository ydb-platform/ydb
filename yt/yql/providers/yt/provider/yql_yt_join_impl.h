#pragma once

#include <yql/essentials/core/yql_join.h>

#include <yt/yql/providers/yt/opt/yql_yt_join.h>
#include <yt/yql/providers/yt/provider/yql_yt_provider.h>
#include <yt/yql/providers/yt/expr_nodes/yql_yt_expr_nodes.h>

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
    bool CostBasedOptPassed = false;
};

struct TOptimizerLinkSettings {
    bool HasForceSortedMerge = false;
    bool HasHints = false;
};

TYtJoinNodeOp::TPtr ImportYtEquiJoin(TYtEquiJoin equiJoin, TExprContext& ctx);

IGraphTransformer::TStatus RewriteYtEquiJoinLeaves(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx);
IGraphTransformer::TStatus RewriteYtEquiJoin(TYtEquiJoin equiJoin, TYtJoinNodeOp& op, const TYtState::TPtr& state, TExprContext& ctx);
TMaybeNode<TExprBase> ExportYtEquiJoin(TYtEquiJoin equiJoin, const TYtJoinNodeOp& op, TExprContext& ctx, const TYtState::TPtr& state);
TYtJoinNodeOp::TPtr OrderJoins(TYtJoinNodeOp::TPtr op, const TYtState::TPtr& state, const TString& cluster,  TExprContext& ctx, bool debug = false);

struct IBaseOptimizerNode;
struct IProviderContext;

void BuildOptimizerJoinTree(TYtState::TPtr state, const TString& cluster, std::shared_ptr<IBaseOptimizerNode>& tree, std::shared_ptr<IProviderContext>& providerCtx, TOptimizerLinkSettings& settings, TYtJoinNodeOp::TPtr op, TExprContext& ctx);
TYtJoinNode::TPtr BuildYtJoinTree(std::shared_ptr<IBaseOptimizerNode> node, TExprContext& ctx, TPositionHandle pos);

bool AreSimilarTrees(TYtJoinNode::TPtr node1, TYtJoinNode::TPtr node2);

IGraphTransformer::TStatus CollectPathsAndLabelsReady(
    bool& ready, TVector<TYtPathInfo::TPtr>& tables, TJoinLabels& labels,
    const TStructExprType*& itemType, const TStructExprType*& itemTypeBeforePremap,
    const TYtJoinNodeLeaf& leaf, TExprContext& ctx);

IGraphTransformer::TStatus CalculateJoinLeafSize(ui64& result, TMapJoinSettings& settings, TYtSection& inputSection,
    const TYtJoinNodeOp& op, TExprContext& ctx, bool isLeft,
    const TStructExprType* itemType, const TVector<TString>& joinKeyList, const TYtState::TPtr& state, const TString& cluster,
    const TVector<TYtPathInfo::TPtr>& tables);

enum class ESizeStatCollectMode {
    NoSize,
    RawSize,
    ColumnarSize,
};

struct TJoinSideStats {
    TString TableNames;
    bool HasUniqueKeys = false;
    bool IsDynamic = false;
    bool NeedsRemap = false;

    TVector<TString> SortedKeys;

    ui64 RowsCount = 0;
    ui64 Size = 0;
};

IGraphTransformer::TStatus CollectStatsAndMapJoinSettings(ESizeStatCollectMode sizeMode, TMapJoinSettings& mapSettings,
    TJoinSideStats& leftStats, TJoinSideStats& rightStats,
    bool leftTablesReady, const TVector<TYtPathInfo::TPtr>& leftTables, const THashSet<TString>& leftJoinKeys,
    bool rightTablesReady, const TVector<TYtPathInfo::TPtr>& rightTables, const THashSet<TString>& rightJoinKeys,
    TYtJoinNodeLeaf* leftLeaf, TYtJoinNodeLeaf* rightLeaf, const TYtState& state, bool isCross,
    TString cluster, TExprContext& ctx);

IGraphTransformer::TStatus TryEstimateDataSizeChecked(TVector<ui64>& result, TYtSection& inputSection, const TString& cluster,
    const TVector<TYtPathInfo::TPtr>& paths, const TMaybe<TVector<TString>>& columns, const TYtState& state, TExprContext& ctx);

ui64 CalcInMemorySizeNoCrossJoin(const TJoinLabel& label, const TYtJoinNodeOp& op, const TMapJoinSettings& settings, bool isLeft,
    TExprContext& ctx, bool needPayload, ui64 size);

}
