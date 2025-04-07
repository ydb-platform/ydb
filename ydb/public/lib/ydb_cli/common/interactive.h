#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb {
namespace NConsoleClient {

bool AskYesOrNo();

bool AskPrompt(const std::string &query, bool defaultAnswer);

bool IsStdinInteractive();

bool IsStdoutInteractive();

std::optional<size_t> GetTerminalWidth();

}
}
