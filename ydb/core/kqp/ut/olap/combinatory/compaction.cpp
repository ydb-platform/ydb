#include "compaction.h"

#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/kqp/ut/olap/combinatory/execute.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr::NKqp {

TConclusionStatus TRestartTabletsCommand::DoExecute(TKikimrRunner& kikimr) {
    auto csController = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    for (auto&& i : csController->GetShardActualIds()) {
        kikimr.GetTestServer().GetRuntime()->Send(
            MakePipePerNodeCacheID(false), NActors::TActorId(), new TEvPipeCache::TEvForward(new TEvents::TEvPoisonPill(), i, false));
    }
    return TConclusionStatus::Success();
}

TConclusionStatus TStopSchemasCleanupCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::CleanupSchemas);
    return TConclusionStatus::Success();
}

TConclusionStatus TOneSchemasCleanupCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    AFL_VERIFY(!controller->IsBackgroundEnable(NKikimr::NYDBTest::ICSController::EBackground::CleanupSchemas));
    const i64 cleanups = controller->GetCleanupSchemasFinishedCounter().Val();
    controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::CleanupSchemas);
    const TInstant start = TInstant::Now();
    while (TInstant::Now() - start < TDuration::Seconds(30)) {
        if (cleanups < controller->GetCleanupSchemasFinishedCounter().Val()) {
            Cerr << "SCHEMAS_CLEANUP_HAPPENED: " << cleanups << " -> " << controller->GetCleanupSchemasFinishedCounter().Val() << Endl;
            break;
        }

        Cerr << "WAIT_SCHEMAS_CLEANUP: " << controller->GetCleanupSchemasFinishedCounter().Val() << Endl;
        Sleep(TDuration::MilliSeconds(300));
    }

    if (Expected) {
        AFL_VERIFY((cleanups < controller->GetCleanupSchemasFinishedCounter().Val()) == *Expected);
    }

    controller->DisableBackground(NKikimr::NYDBTest::ICSController::EBackground::CleanupSchemas);
    return TConclusionStatus::Success();
}

TConclusionStatus TFastPortionsCleanupCommand::DoExecute(TKikimrRunner& /*kikimr*/) {
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    controller->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
    controller->SetOverrideMaxReadStaleness(TDuration::Seconds(1));
    return TConclusionStatus::Success();
}

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
    // TSchemaCommand command;
    // command.DeserializeFromString("ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`,`COMPACTION_PLANNER.FEATURES`=`{'accumulator_trigger_portions':2}`)");
    // AFL_VERIFY(command.Execute(kikimr).IsSuccess());
    const i64 compactions = controller->GetCompactionFinishedCounter().Val();
    controller->EnableBackground(NKikimr::NYDBTest::ICSController::EBackground::Compaction);
    const TInstant start = TInstant::Now();
    while (TInstant::Now() - start < TDuration::Seconds(60)) {
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
