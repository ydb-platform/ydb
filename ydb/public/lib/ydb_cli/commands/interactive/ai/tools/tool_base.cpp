#include "tool_base.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

namespace NYdb::NConsoleClient::NAi {

TToolBase::TToolBase(const NJson::TJsonValue& parametersSchema, const TString& description, const TInteractiveLogger& log)
    : Log(log)
    , ParametersSchema(parametersSchema)
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
        Log.Warning() << "Failed to parse parameters of tool: " << e.what();
        return TResponse(TStringBuilder() << "Failed to parse parameters of tool: " << e.what() << "\n" << "Parameters schema: " << FormatJsonValue(ParametersSchema));
    }

    if (!AskPermissions()) {
        Log.Notice() << "Tool execution cancelled by user";
        return TResponse(TStringBuilder() << "Tool execution cancelled by user");
    }

    try {
        return DoExecute();
    } catch (const std::exception& e) {
        Log.Warning() << "Failed to execute tool: " << e.what();
        return TResponse(TStringBuilder() << "Failed to execute tool: " << e.what());
    }
}

} // namespace NYdb::NConsoleClient::NAi
