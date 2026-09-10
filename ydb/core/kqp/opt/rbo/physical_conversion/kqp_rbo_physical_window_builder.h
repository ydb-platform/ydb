#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"
#include <yql/essentials/core/yql_opt_utils.h>
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalWindowBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalWindowBuilder(TIntrusivePtr<TOpWindow> window, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , Window(window) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;
    static bool CanBuildWindow(const TOpWindow& window);

private:
    void Prepare(const TVector<TInfoUnit>& inputs);
    ui32 IndexOf(const TInfoUnit& column) const;
    const TTypeAnnotationNode* InputItemType(const TInfoUnit& column) const;

    TVector<TExprNode::TPtr> BuildSortKeys() const;
    TExprNode::TPtr BuildKeyExtractorLambda() const;
    TExprNode::TPtr BuildGroupSwitchLambda() const;

    TExprNode::TPtr BuildChain(TExprNode::TPtr wideFlow) const;
    TExprNode::TPtr BuildChainLambda(bool update) const;
    TExprNode::TPtr BuildExpandFromChain(TExprNode::TPtr chained) const;

    TString AccumulatorName(ui32 funcIndex) const;
    TString PositionName(ui32 funcIndex) const;
    TString PeerName(ui32 sortIndex) const;

    TExprNode::TPtr Member(TExprNode::TPtr from, const TString& name) const;
    TExprNode::TPtr BuildStruct(const TVector<std::pair<TString, TExprNode::TPtr>>& members) const;
    TExprNode::TPtr BuildSumCastTarget(const TInfoUnit& column) const;
    TExprNode::TPtr MakeOptional(TExprNode::TPtr value, bool alreadyOptional) const;
    TExprNode::TPtr BuildUint64(ui64 value) const;

    TIntrusivePtr<TOpWindow> Window;
    TVector<TInfoUnit> Inputs;
    THashMap<TString, ui32> Indexes;
    const TStructExprType* InputStruct = nullptr;
    TVector<TInfoUnit> OutputLayout;
    bool NeedsPeerKey = false;
};
