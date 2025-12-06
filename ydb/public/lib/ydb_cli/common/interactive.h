#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient {

void AskInputWithPrompt(const TString& prompt, std::function<bool(const TString&)> handler, bool verbose = false);

void AskAnyInputWithPrompt(const TString& prompt, std::function<void(const TString&)> handler, bool verbose = false);

TString AskAnyInputWithPrompt(const TString& prompt, bool verbose = false);

bool AskYesOrNo(const TString& query, std::optional<bool> defaultAnswer = std::nullopt);

bool IsStdinInteractive();

bool IsStdoutInteractive();

std::optional<size_t> GetTerminalWidth();

} // namespace NYdb::NConsoleClient
