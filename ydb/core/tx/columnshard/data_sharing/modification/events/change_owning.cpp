#include "change_owning.h"
#include <ydb/core/tx/columnshard/data_sharing/modification/tasks/modification.h>

namespace NKikimr::NOlap::NDataSharing::NEvents {

TEvApplyLinksModification::TEvApplyLinksModification(const TTabletId initiatorTabletId, const TString& sessionId, const ui64 packIdx, const TTaskForTablet& task) {
    Record.SetInitiatorTabletId((ui64)initiatorTabletId);
    Record.SetSessionId(sessionId);
    Record.SetPackIdx(packIdx);
    *Record.MutableTask() = task.SerializeToProto();
}

}