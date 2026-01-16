#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

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

} // namespace NYdb::NConsoleClient::NAi
