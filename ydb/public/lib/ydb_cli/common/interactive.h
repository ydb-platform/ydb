#pragma once

#include <ydb-cpp-sdk/client/ydb_scheme/scheme.h>

namespace NYdb {
namespace NConsoleClient {

bool AskYesOrNo();

bool IsStdinInteractive();

bool IsStdoutInteractive();

std::optional<size_t> GetTerminalWidth();

}
}
