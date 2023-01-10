#include "kqp_compute_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/library/yql/core/issue/yql_issue.h>

namespace NKikimr::NKqp::NComputeActor {

bool FindSchemeErrorInIssues(const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues) {
    bool schemeError = false;
    if (status == Ydb::StatusIds::SCHEME_ERROR) {
        schemeError = true;
    } else if (status == Ydb::StatusIds::ABORTED) {
        for (auto& issue : issues) {
            WalkThroughIssues(issue, false, [&schemeError](const NYql::TIssue& x, ui16) {
                if (x.IssueCode == NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH) {
                    schemeError = true;
                }
            });
            if (schemeError) {
                break;
            }
        }
    }
    return schemeError;
}

void FillTaskInputStats(const NYql::NDqProto::TDqTask& task, NYql::NDqProto::TDqTaskStats& taskStats) {
    THashMap<ui32, TString> inputTables;

    for (ui32 inputIndex = 0; inputIndex < task.InputsSize(); ++inputIndex) {
        const auto& taskInput = task.GetInputs(inputIndex);
        if (taskInput.HasTransform()) {
            const auto& transform = taskInput.GetTransform();
            YQL_ENSURE(transform.GetType() == "StreamLookupInputTransformer",
                "Unexpected input transform type: " << transform.GetType());

            const google::protobuf::Any &settingsAny = transform.GetSettings();
            YQL_ENSURE(settingsAny.Is<NKikimrKqp::TKqpStreamLookupSettings>(), "Expected settings type: "
                << NKikimrKqp::TKqpStreamLookupSettings::descriptor()->full_name()
                << " , but got: " << settingsAny.type_url());

            NKikimrKqp::TKqpStreamLookupSettings settings;
            YQL_ENSURE(settingsAny.UnpackTo(&settings), "Failed to unpack settings");

            inputTables.insert({inputIndex, settings.GetTable().GetPath()});
        }
    }

    for (const auto& transformerStats : taskStats.GetInputTransforms()) {
        auto tableIt = inputTables.find(transformerStats.GetInputIndex());
        YQL_ENSURE(tableIt != inputTables.end());

        auto* tableStats = taskStats.AddTables();
        tableStats->SetTablePath(tableIt->second);
        tableStats->SetReadRows(transformerStats.GetRowsOut());
        tableStats->SetReadBytes(transformerStats.GetBytes());
    }
}

} // namespace NKikimr::NKqp::NComputeActor
