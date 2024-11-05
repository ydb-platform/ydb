#include "session.h"

#include <ydb/core/tx/columnshard/bg_tasks/abstract/adapter.h>
#include <ydb/core/tx/columnshard/columnshard_impl.h>
#include <ydb/core/tx/columnshard/export/actor/export_actor.h>

namespace NKikimr::NOlap::NExport {

NKikimr::TConclusion<std::unique_ptr<NActors::IActor>> TSession::DoCreateActor(const NBackground::TStartContext& context) const {
    AFL_VERIFY(IsConfirmed());
    auto blobsOperator = Task->GetStorageInitializer()->InitializeOperator(context.GetAdapter()->GetTabletExecutorVerifiedAs<NColumnShard::TColumnShard>().GetStoragesManager());
    if (blobsOperator.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("event", "problem_on_export_start")("reason", "cannot_initialize_operator")("problem", blobsOperator.GetErrorMessage());
        return TConclusionStatus::Fail("cannot initialize blobs operator");
    }
    Status = EStatus::Started;
    return std::make_unique<TActor>(context.GetSessionSelfPtr(), context.GetAdapter(), blobsOperator.DetachResult());
}

}   // namespace NKikimr::NOlap::NExport
