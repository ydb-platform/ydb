#pragma once

#include <util/generic/string.h>
#include <vector>
#include <functional>
#include <optional>

namespace NYdb::NConsoleClient {

using TMenuEntry = std::pair<TString, std::function<void()>>;

std::optional<size_t> RunFtxuiMenu(const TString& title, const std::vector<TString>& options);

bool RunFtxuiMenuWithActions(const TString& title, const std::vector<TMenuEntry>& options);

std::optional<TString> RunFtxuiInput(const TString& title, const TString& initial, const std::function<bool(const TString&, TString&)>& validator);

bool AskYesNoFtxui(const TString& question, bool defaultAnswer = false);

void PrintFtxuiMessage(const TString& message, const TString& title = "");

} // namespace NYdb::NConsoleClient


