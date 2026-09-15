#pragma once
#include "kqp_rbo_physical_op_builder.h"
#include "kqp_rbo_physical_convertion_utils.h"

using namespace NYql::NNodes;
using namespace NKikimr;
using namespace NKikimr::NKqp;

namespace NKikimr::NKqp::NLookupJoinBuilder {

struct TLookupKeysResult {
    NYql::TExprNode::TPtr InputStage;
    NYql::TExprNode::TPtr InputType;
};

TLookupKeysResult BuildLookupKeys(TOpTableLookup& lookup, NYql::TExprNode::TPtr inputStage, NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp::NLookupJoinBuilder

class TPhysicalIndexLookupJoinBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalIndexLookupJoinBuilder(TIntrusivePtr<TOpIndexLookupJoin> lookupJoin, TExprContext& ctx, TPositionHandle pos)
        : TPhysicalUnaryOpBuilder(ctx, pos)
        , LookupJoin(lookupJoin) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TExprNode::TPtr ProcessFetchedRows(TExprNode::TPtr input, const TOpTableLookup& lookup) const;
    TExprNode::TPtr BuildRenamedRow(const TExprBase& fetchedRow, const TOpTableLookup& lookup, bool& needsRename) const;

    TIntrusivePtr<TOpIndexLookupJoin> LookupJoin;
};
