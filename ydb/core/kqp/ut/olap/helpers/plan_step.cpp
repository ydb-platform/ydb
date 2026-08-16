#include "plan_step.h"

#include <ydb/core/protos/flat_scheme_op.pb.h>

#include <ydb/library/actors/core/log.h>

namespace NKikimr::NKqp {

namespace {
void ExecuteAlter(TKikimrRunner& kikimr, const TString& alterQuery) {
    auto session = kikimr.GetTableClient().CreateSession().GetValueSync().GetSession();
    auto alterResult = session.ExecuteSchemeQuery(alterQuery).GetValueSync();
    AFL_VERIFY(alterResult.GetStatus() == NYdb::EStatus::SUCCESS)("error", alterResult.GetIssues().ToString());
}
}   // namespace

// The plan step is database-global (it reaches every shard via mediator time cast), so a single schema change
// on any one column object is enough.
void AdvancePlanStep(TKikimrRunner& kikimr, const TString& root) {
    auto description = kikimr.GetTestClient().Ls(root);
    AFL_VERIFY(description);
    for (const auto& child : description->Record.GetPathDescription().GetChildren()) {
        TString objectType;
        switch (child.GetPathType()) {
            case NKikimrSchemeOp::EPathTypeColumnTable:
                objectType = "TABLE";
                break;
            case NKikimrSchemeOp::EPathTypeColumnStore:
                objectType = "TABLESTORE";
                break;
            default:
                continue;
        }
        ExecuteAlter(kikimr, TStringBuilder() << "ALTER OBJECT `" << root << "/" << child.GetName() << "` (TYPE "
                                              << objectType << ") SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`false`);");
        return;
    }
    AFL_VERIFY(false)("error", "no column tables/stores found to advance the plan step")("root", root);
}

}   // namespace NKikimr::NKqp
