#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb {
namespace NConsoleClient {

bool AskYesOrNo();

bool AskPrompt(const std::string &query, bool defaultAnswer);

bool IsStdinInteractive();

bool IsStdoutInteractive();

bool IsStderrInteractive();

std::optional<size_t> GetTerminalWidth();
std::optional<size_t> GetErrTerminalWidth();

}
}
