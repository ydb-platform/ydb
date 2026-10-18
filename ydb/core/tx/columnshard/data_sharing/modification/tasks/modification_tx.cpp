#include "modification.h"

#include <ydb/core/tx/columnshard/data_sharing/modification/transactions/tx_change_blobs_owning.h>

namespace NKikimr::NOlap::NDataSharing {

NKikimr::TConclusion<std::unique_ptr<NKikimr::NTabletFlatExecutor::ITransaction>> TTaskForTablet::BuildModificationTransaction(
    NColumnShard::TColumnShard* self, const TTabletId initiator, const TString& sessionId, const ui64 packIdx,
    const std::shared_ptr<TTaskForTablet>& selfPtr) {
    return std::unique_ptr<NTabletFlatExecutor::ITransaction>(new TTxApplyLinksModification(self, selfPtr, sessionId, initiator, packIdx));
}

}   // namespace NKikimr::NOlap::NDataSharing
