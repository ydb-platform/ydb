#include "columnshard_impl.h"

#include <ydb/core/tx/columnshard/blobs_action/bs/history_cutter.h>
#include <ydb/core/tx/columnshard/blobs_action/bs/storage.h>

#include <ydb/library/actors/core/actor.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NColumnShard {

void TColumnShard::SetupCutHistory() {
    if (CutHistoryCutter) {
        CutHistoryCutter->RequestNomination(/*triggered=*/false);
        return;
    }
    auto op = std::dynamic_pointer_cast<NOlap::NBlobOperations::NBlobStorage::TOperator>(
        StoragesManager->GetOperatorOptional(NOlap::IStoragesManager::DefaultStorageId));
    if (!op) {
        return;
    }
    op->InitHistoryCutter(SelfId());
    auto* cutter = op->GetHistoryCutter();
    if (!cutter) {
        return;
    }
    cutter->SetLauncherActorId(LauncherID());
    CutHistoryCutter = cutter;
    cutter->RequestNomination(/*triggered=*/false);
}

void TColumnShard::Handle(TEvPrivate::TEvCutHistoryNominate::TPtr& /*ev*/, const TActorContext& ctx) {
    if (!CutHistoryCutter) {
        return;
    }
    CutHistoryCutter->OnNominationEvent(ctx);
}

}   // namespace NKikimr::NColumnShard
