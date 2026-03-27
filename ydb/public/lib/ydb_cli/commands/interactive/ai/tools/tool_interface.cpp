#include "tool_interface.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

namespace NYdb::NConsoleClient::NAi {

ITool::TResponse ITool::TResponse::Error(const TString& error, const TString& userMessage) {
    Y_VALIDATE(!error.empty(), "Error message can not be empty for erro response");
    return TResponse(error, userMessage, /* isSuccess */ false);
}

ITool::TResponse ITool::TResponse::Success(const TString& result, const TString& userMessage) {
    return TResponse(result, userMessage, /* isSuccess */ true);
}

ITool::TResponse ITool::TResponse::Success(const NJson::TJsonValue& result, const TString& userMessage) {
    return TResponse(FormatJsonValue(result), userMessage, /* isSuccess */ true);
}

ITool::TResponse::TResponse(const TString& result, const TString& userMessage, bool isSuccess)
    : UserMessage(userMessage)
    , ToolResult(result)
    , IsSuccess(isSuccess)
{}

} // namespace NYdb::NConsoleClient::NAi
