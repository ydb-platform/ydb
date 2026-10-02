#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/ydb_path.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

namespace {

TString CanonizeAbsolutePath(const TString& path) {
    auto canonical = CanonizeYdbPath(path);
    return canonical.empty() ? TString("/") : canonical;
}

} // anonymous namespace

TToolBase::TToolBase(const NJson::TJsonValue& parametersSchema, const TString& description)
    : ParametersSchema(parametersSchema)
    , Description(description)
{}

void TToolBase::SetAutoAction(TInteractiveConfigurationManager::EToolAutoAction autoAction) {
    AutoAction = autoAction;
}

const NJson::TJsonValue& TToolBase::GetParametersSchema() const {
    return ParametersSchema;
}

const TString& TToolBase::GetDescription() const {
    return Description;
}

TToolBase::TResponse TToolBase::Execute(const NJson::TJsonValue& parameters) {
    YDB_CLI_LOG(Debug, "Execution tool with params:\n" << FormatJsonValue(parameters));

    try {
        ParseParameters(parameters);
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Warning, "Failed to parse parameters of tool: " << e.what() << "\nParameters:\n" << FormatJsonValue(parameters));
        return TResponse::Error(TStringBuilder() << "Failed to parse parameters of tool: " << e.what());
    }

    if (AutoAction == TInteractiveConfigurationManager::EToolAutoAction::Reject) {
        return TResponse::Error(TString("Tool execution is not allowed, try to use other tools"));
    }

    if (AutoAction != TInteractiveConfigurationManager::EToolAutoAction::Execute && !AskPermissions()) {
        YDB_CLI_LOG(Notice, "Tool execution cancelled by user");
        return TResponse::Error(TString("Tool execution cancelled by user"));
    }

    try {
        return DoExecute();
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Warning, "Failed to execute tool: " << e.what());
        return TResponse::Error(TStringBuilder() << "Failed to execute tool: " << e.what());
    }
}

TDatabaseToolBase::TDatabaseToolBase(const TString& database, const NJson::TJsonValue& parametersSchema, const TString& description)
    : TBase(parametersSchema, description)
    , Database(database.StartsWith('/') ? CanonizeAbsolutePath(database) : database)
{}

TString TDatabaseToolBase::CanonizePath(const TString& path) const {
    const auto result = Strip(path);
    if (result.StartsWith('/')) {
        return CanonizeAbsolutePath(result);
    }
    if (Database.StartsWith('/')) {
        return JoinYdbPath({Database, result});
    }

    // Only the server can resolve a relative database against the cluster root.
    return result;
}

} // namespace NYdb::NConsoleClient::NAi
