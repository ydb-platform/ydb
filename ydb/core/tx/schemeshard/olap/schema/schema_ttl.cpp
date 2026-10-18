#include "schema.h"

#include <ydb/core/tx/schemeshard/olap/ttl/validator.h>

namespace NKikimr::NSchemeShard {

bool TOlapSchema::ValidateTtlSettings(
    const NKikimrSchemeOp::TColumnDataLifeCycle& ttl, const TOperationContext& context, IErrorCollector& errors) const {
    using TTtlProto = NKikimrSchemeOp::TColumnDataLifeCycle;
    switch (ttl.GetStatusCase()) {
        case TTtlProto::kEnabled: 
        {
            const auto* column = Columns.GetByName(ttl.GetEnabled().GetColumnName());
            if (!column) {
                errors.AddError("Incorrect ttl column - not found in scheme");
                return false;
            }
            return TTTLValidator::ValidateColumnTableTtl(ttl.GetEnabled(), Indexes, {}, Columns.GetColumns(), Columns.GetColumnsByName(), context, errors);
        }
        case TTtlProto::kDisabled:
        default:
            break;
    }

    return true;
}

}   // namespace NKikimr::NSchemeShard
