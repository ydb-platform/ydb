#include "action.h"
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap {

std::shared_ptr<NKikimr::NOlap::IBlobsWritingAction> TBlobsAction::GetWriting(const TPortionInfo& portionInfo) {
    return GetStorageAction(portionInfo.GetBlobsStorage()->GetStorageId()).GetWriting();
}

std::shared_ptr<NKikimr::NOlap::IBlobsReadingAction> TBlobsAction::GetReading(const TPortionInfo& portionInfo) {
    return GetStorageAction(portionInfo.GetBlobsStorage()->GetStorageId()).GetReading();
}

std::shared_ptr<NKikimr::NOlap::IBlobsDeclareRemovingAction> TBlobsAction::GetRemoving(const TPortionInfo& portionInfo) {
    return GetStorageAction(portionInfo.GetBlobsStorage()->GetStorageId()).GetRemoving();
}

}
