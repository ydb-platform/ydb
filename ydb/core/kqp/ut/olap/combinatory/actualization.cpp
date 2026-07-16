#include "actualization.h"

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
    // Actualization commits the rewritten (e.g. re-indexed) portions at GetOutdatedStep()+1 (LastPlannedStep+1),
    // so that no active scans can see that.
    // But we want to be sure our next test scans/reads see the actualized portions, so issue a benign, non-critical schema change
    // that will move the plan step forward.
    ExecuteAlter(kikimr, "ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`false`);");
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
