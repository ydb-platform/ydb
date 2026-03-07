#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/ydb_path.h>

#include <util/string/strip.h>

namespace NYdb::NConsoleClient::NAi {

TToolBase::TToolBase(const NJson::TJsonValue& parametersSchema, const TString& description)
    : ParametersSchema(parametersSchema)
    , Description(description)
{}

const NJson::TJsonValue& TToolBase::GetParametersSchema() const {
    return ParametersSchema;
}

const TString& TToolBase::GetDescription() const {
    return Description;
}

TToolBase::TResponse TToolBase::Execute(const NJson::TJsonValue& parameters) {
    try {
        ParseParameters(parameters);
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Warning, "Failed to parse parameters of tool: " << e.what());
        return TResponse::Error(TStringBuilder() << "Failed to parse parameters of tool: " << e.what() << "\n" << "Parameters schema: " << FormatJsonValue(ParametersSchema));
    }

    if (!AskPermissions()) {
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
    , Database(CanonizeYdbPath(database))
{}

TString TDatabaseToolBase::CanonizePath(const TString& path) const {
    auto result = Strip(path);
    if (!result.StartsWith('/' || !result.StartsWith(Database))) {
        // If path starts with '/' but not with Database prefix, assume it is relative to Database.
        // This is a common confusion for AI agents who see file lists without full path prefix.
        result = JoinYdbPath({Database, result});
    }

    return CanonizeYdbPath(result);
}

} // namespace NYdb::NConsoleClient::NAi
