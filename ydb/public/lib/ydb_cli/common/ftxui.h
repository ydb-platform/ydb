#pragma once

#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>
#include <contrib/restricted/patched/replxx/include/replxx.hxx>

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

std::optional<TString> RunFtxuiInput(
    const TString& title,
    const TString& initial = "",
    const std::function<bool(const TString&, TString&)>& validator = {},
    const TString& placeholder = "");

std::optional<TString> RunFtxuiInputWithSuffix(
    const TString& title,
    const TString& initial,
    const TString& suffix,
    const std::function<bool(const TString&, TString&)>& validator = {},
    const TString& placeholder = "");

std::optional<bool> AskYesNoFtxuiOptional(const ftxui::Element& question, bool defaultAnswer = false, std::optional<ftxui::Color> color = std::nullopt);

bool AskYesNoFtxui(const TString& question, bool defaultAnswer = false);

// Password input with masking (shows asterisks)
std::optional<TString> RunFtxuiPasswordInput(ftxui::Element title);

std::optional<TString> RunFtxuiPasswordInput(const TString& title);

void PrintFtxuiMessage(std::optional<ftxui::Element> message, const TString& title = "", ftxui::Color color = ftxui::Color::Cyan);

void PrintFtxuiMessage(const TString& body, const TString& title = "", ftxui::Color color = ftxui::Color::Cyan);

ftxui::Color ToFtxuiColor(replxx::Replxx::Color rColor);

void FlushStdin();

} // namespace NYdb::NConsoleClient
