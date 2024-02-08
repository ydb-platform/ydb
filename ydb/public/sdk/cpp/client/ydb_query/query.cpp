#include "query.h"

namespace NYdb::NQuery {

std::optional<EStatsMode> ParseStatsMode(std::string_view statsMode) {
    if (statsMode == "unspecified") {
        return EStatsMode::Unspecified;
    } else if (statsMode == "none") {
        return EStatsMode::None;
    } else if (statsMode == "basic") {
        return EStatsMode::Basic;
    } else if (statsMode == "full") {
        return EStatsMode::Full;
    } else if (statsMode == "profile") {
        return EStatsMode::Profile;
    }

    return {};
}

std::string_view StatsModeToString(const EStatsMode statsMode) {
    switch (statsMode) {
    case EStatsMode::Unspecified:
        return "unspecified";
    case EStatsMode::None:
        return "none";
    case EStatsMode::Basic:
        return "basic";
    case EStatsMode::Full:
        return "full";
    case EStatsMode::Profile:
        return "profile";
    }
}

TScriptExecutionOperation::TScriptExecutionOperation(TStatus&& status, Ydb::Operations::Operation&& operation)
    : TOperation(std::move(status), std::move(operation))
{
    Ydb::Query::ExecuteScriptMetadata metadata;
    GetProto().metadata().UnpackTo(&metadata);

    Metadata_.ExecutionId = metadata.execution_id();
    Metadata_.ExecMode = static_cast<EExecMode>(metadata.exec_mode());
    Metadata_.ExecStatus = static_cast<EExecStatus>(metadata.exec_status());
    Metadata_.ExecStats = metadata.exec_stats();
    Metadata_.ResultSetsMeta.insert(Metadata_.ResultSetsMeta.end(), metadata.result_sets_meta().begin(), metadata.result_sets_meta().end());

    if (metadata.has_script_content()) {
        Metadata_.ScriptContent.Syntax = static_cast<ESyntax>(metadata.script_content().syntax());
        Metadata_.ScriptContent.Text = metadata.script_content().text();
    }
}

TCommitTransactionResult::TCommitTransactionResult(TStatus&& status)
    : TStatus(std::move(status))
{}

} // namespace NYdb::NQuery
