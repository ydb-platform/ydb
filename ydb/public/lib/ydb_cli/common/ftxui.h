#pragma once

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>

#include <util/generic/string.h>

#include <functional>
#include <optional>
#include <vector>

namespace NYdb::NConsoleClient {

using TMenuEntry = std::pair<TString, std::function<void()>>;

// Set the global border color for all FTXUI menus/dialogs
void SetFtxuiBorderColor(ftxui::Color color);

// Get the current global border color
ftxui::Color GetFtxuiBorderColor();

std::optional<size_t> RunFtxuiMenu(const TString& title, const std::vector<TString>& options, size_t initialSelected = 0, size_t maxPageSize = 20);

bool RunFtxuiMenuWithActions(const TString& title, const std::vector<TMenuEntry>& options, size_t maxPageSize = 20);

std::optional<TString> RunFtxuiInput(const TString& title, const TString& initial, const std::function<bool(const TString&, TString&)>& validator);

std::optional<TString> RunFtxuiInputWithSuffix(
    const TString& title,
    const TString& initial,
    const TString& suffix,
    const std::function<bool(const TString&, TString&)>& validator);

bool AskYesNoFtxui(const TString& question, bool defaultAnswer = false);

void PrintFtxuiMessage(std::optional<ftxui::Element> message, const TString& title = "", ftxui::Color color = ftxui::Color::Cyan);

void PrintFtxuiMessage(const TString& body, const TString& title = "", ftxui::Color color = ftxui::Color::Cyan);

} // namespace NYdb::NConsoleClient


