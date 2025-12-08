#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/commands/interactive/common/json_utils.h>

namespace NYdb::NConsoleClient::NAi {

ITool::TResponse::TResponse(const TString& error, const TString& userMessage)
    : UserMessage(userMessage)
    , ToolResult(error)
    , IsSuccess(false)
{}

ITool::TResponse::TResponse(const NJson::TJsonValue& result, const TString& userMessage)
    : UserMessage(userMessage)
    , ToolResult(FormatJsonValue(result))
    , IsSuccess(true)
{}

} // namespace NYdb::NConsoleClient::NAi
