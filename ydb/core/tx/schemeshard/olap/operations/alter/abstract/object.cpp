#include "object.h"
#include <ydb/core/tx/schemeshard/schemeshard_impl.h>

namespace NKikimr::NSchemeShard::NOlap::NAlter {

std::shared_ptr<NKikimr::NSchemeShard::NOlap::NAlter::ISSEntity> ISSEntity::GetEntityVerified(TOperationContext& context, const TPath& path) {
    if (path->IsColumnTable()) {
        auto readGuard = context.SS->ColumnTables.GetVerified(path.Base()->PathId);
        TEntityInitializationContext iContext(&context);
        return readGuard->BuildEntity(path.Base()->PathId, iContext).DetachResult();
    }
    AFL_VERIFY(false);
    return nullptr;
}

}