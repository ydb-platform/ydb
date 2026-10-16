#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include <yql/essentials/utils/log/log.h>

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

class TPhysicalSourceBuilder: public TPhysicalNullaryOpBuilder {
public:
    TPhysicalSourceBuilder(TOpRead& read, TExprContext& ctx, TPositionHandle pos, const TPhysicalNames& names,
        const TInfoUnitRegistry& registry, const TString& stageGUID, bool isSysView = false, TString carrierColumn = {})
        : TPhysicalNullaryOpBuilder(ctx, pos, names)
        , Read(read)
        , Registry(registry)
        , StageGUID(stageGUID)
        , IsSysView(isSysView)
        , CarrierColumn(std::move(carrierColumn)) {}

    TExprNode::TPtr BuildPhysicalOp() override;

private:
    TOpRead& Read;
    const TInfoUnitRegistry& Registry;
    TString StageGUID;
    bool IsSysView;
    TString CarrierColumn;
};
