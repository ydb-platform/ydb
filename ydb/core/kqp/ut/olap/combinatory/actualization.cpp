#include "actualization.h"

#include <ydb/core/tx/columnshard/hooks/abstract/abstract.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>

namespace NKikimr::NKqp {

TConclusionStatus TOneActualizationCommand::DoExecute(TKikimrRunner& kikimr) {
    {
        auto alterQuery =
            TStringBuilder() << "ALTER OBJECT `/Root/ColumnTable` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);";
        auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
        auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
        AFL_VERIFY(alterResult.GetStatus() == NYdb::EStatus::SUCCESS)("error", alterResult.GetIssues().ToString());
    }
    auto controller = NYDBTest::TControllers::GetControllerAs<NYDBTest::NColumnShard::TController>();
    AFL_VERIFY(controller);
    controller->WaitActualization(TDuration::Seconds(10));
    return TConclusionStatus::Success();
}

}   // namespace NKikimr::NKqp
