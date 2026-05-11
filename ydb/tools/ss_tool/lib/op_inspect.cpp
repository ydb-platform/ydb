#include "op_inspect.h"

#include <ydb/core/tx/schemeshard/generated/op_handlers.h>

#include <util/generic/hash_set.h>

namespace NKikimr::NSchemeShard::NSsTool {

TOpRow CollectRow(NKikimrSchemeOp::EOperationType opType) {
    TOpRow row;
    row.Type = opType;
    row.Name = NKikimrSchemeOp::EOperationType_Name(opType);
    row.IsRegistered = NGenerated::NOpHandlers::IsOpRegistered(opType);
    return row;
}

TVector<TOpRow> AllOps() {
    TVector<TOpRow> result;
    THashSet<int> seen; // proto enum may carry aliases sharing one number
    const auto* d = NKikimrSchemeOp::EOperationType_descriptor();
    for (int i = 0; i < d->value_count(); ++i) {
        const auto* v = d->value(i);
        if (!seen.insert(v->number()).second) {
            continue;
        }
        result.push_back(CollectRow(static_cast<NKikimrSchemeOp::EOperationType>(v->number())));
    }
    return result;
}

} // namespace NKikimr::NSchemeShard::NSsTool
