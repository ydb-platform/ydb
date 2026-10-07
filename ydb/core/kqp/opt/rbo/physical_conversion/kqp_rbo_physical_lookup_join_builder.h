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
    // Type of the left rows passed through the lookup.
    const NYql::TStructExprType* LeftRowType = nullptr;
};

TLookupKeysResult BuildLookupKeys(TOpTableLookup& lookup, NYql::TExprNode::TPtr inputStage, NYql::TExprContext& ctx,
    const TPhysicalNames& names);

// Builds the input type of a lookup by keys of the input lookup: the output of the input lookup is passed
// as is, so its fetched rows are the lookup keys. The fetched columns are named by display name in the
// logical type and by storage name in the key tuple, so both name sources are needed.
NYql::TExprNode::TPtr BuildKeysFromInputLookupType(const TOpTableLookup& inputLookup, const NYql::TStructExprType* leftRowType,
                                                   const TPhysicalNames& names, const TInfoUnitRegistry& registry,
                                                   NYql::TExprContext& ctx);

} // namespace NKikimr::NKqp::NLookupJoinBuilder

class TPhysicalIndexLookupJoinBuilder: public TPhysicalUnaryOpBuilder {
public:
    TPhysicalIndexLookupJoinBuilder(TOpIndexLookupJoin& lookupJoin, TExprContext& ctx, TPositionHandle pos,
        const TPhysicalNames& names, const TInfoUnitRegistry& registry)
        : TPhysicalUnaryOpBuilder(ctx, pos, names)
        , LookupJoin(lookupJoin)
        , Registry(registry) {
    }

    TExprNode::TPtr BuildPhysicalOp(TExprNode::TPtr input) override;

private:
    TExprNode::TPtr ProcessFetchedRows(TExprNode::TPtr input, const TOpTableLookup& lookup) const;
    TExprNode::TPtr BuildRenamedRow(const TExprBase& fetchedRow, const TOpTableLookup& lookup, bool& needsRename) const;

    TOpIndexLookupJoin& LookupJoin;
    const TInfoUnitRegistry& Registry;
};
