#include "tool_interface.h"

#include <ydb/public/lib/ydb_cli/commands/ydb_ai/common/json_utils.h>

namespace NYdb::NConsoleClient::NAi {

ITool::TResponse::TResponse(const TString& error)
    : Text(error)
    , IsSuccess(false)
{}

ITool::TResponse::TResponse(const NJson::TJsonValue& result)
    : Text(FormatJsonValue(result))
    , IsSuccess(true)
{}

} // namespace NYdb::NConsoleClient::NAi
