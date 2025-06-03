#include "compaction.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr::NKqp {

TConclusionStatus TStopCompactionCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    return TConclusionStatus::Success();
}

TConclusionStatus TOneCompactionCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    AFL_VERIFY(!controller->IsBackgroundEnable(NKikimr::NYDBTest::ICSController::EBackground::Compaction));
    const i64 before = controller->GetCompactionFinishedCounter().Val();
    controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    const bool ok = controller->WaitCompactions(TDuration::Seconds(5));
    AFL_VERIFY(ok);
    const i64 after = controller->GetCompactionFinishedCounter().Val();
    AFL_VERIFY(before < after);
    controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);

    return TConclusionStatus::Success();
}

TConclusionStatus TWaitCompactionCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    controller->WaitCompactions(TDuration::Seconds(15));
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
