#include "actualization.h"

#include <ydb/core/kqp/ut/olap/helpers/plan_step.h>
#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr::NKqp {

namespace {
void ExecuteAlter(TKikimrRunner& kikimr, const TString& alterQuery) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
    AFL_VERIFY(alterResult.GetStatus() == NYdb::EStatus::SUCCESS)("error", alterResult.GetIssues().ToString());
}
}   // namespace

TConclusionStatus TOneActualizationCommand::DoExecute(TKikimrRunner& kikimr) {
    ExecuteAlter(kikimr, "ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    controller->WaitActualization(TDuration::Seconds(10));
    // Make the just-actualized portions visible to the next test scans/reads.
    AdvancePlanStep(kikimr);
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
