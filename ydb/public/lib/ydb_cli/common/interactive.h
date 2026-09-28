#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient {

bool AskYesOrNo(const TString& query, bool defaultAnswer);

bool IsStdinInteractive();

bool IsStdoutInteractive();

bool IsStderrInteractive();

std::optional<size_t> GetTerminalWidth();

std::optional<size_t> GetErrTerminalWidth();

} // namespace NYdb::NConsoleClient
