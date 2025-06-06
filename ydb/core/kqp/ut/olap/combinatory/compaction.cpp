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
    const i64 compactions = controller->GetCompactionFinishedCounter().Val();
    controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    const TInstant start = TInstant::Now();
    while (TInstant::Now() - start < TDuration::Seconds(10)) {
        if (compactions < controller->GetCompactionFinishedCounter().Val()) {
            Cerr << "COMPACTION_HAPPENED: " << compactions << " -> " << controller->GetCompactionFinishedCounter().Val() << Endl;
            break;
        }

        Cerr << "WAIT_COMPACTION: " << controller->GetCompactionFinishedCounter().Val() << Endl;
        Sleep(TDuration::MilliSeconds(300));
    }

    AFL_VERIFY(compactions < controller->GetCompactionFinishedCounter().Val());
    
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
