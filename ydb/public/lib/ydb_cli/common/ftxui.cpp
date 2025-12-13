#include "ftxui.h"

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>

#include <util/stream/output.h>
#include <util/string/builder.h>

#include <thread>
#include <chrono>

#if defined(_unix_)
#include <termios.h>
#include <unistd.h>
#elif defined(_win_)
#include <windows.h>
#endif

namespace NYdb::NConsoleClient {

namespace {

void FlushStdin() {
    // Wait a bit for any pending terminal responses (like CPR) to arrive
    // and disable ECHO to prevent them from being printed to stdout
#if defined(_unix_)
    struct termios t;
    if (tcgetattr(STDIN_FILENO, &t) == 0) {
        struct termios raw = t;
        raw.c_lflag &= ~ECHO;
        tcsetattr(STDIN_FILENO, TCSANOW, &raw);

        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        tcflush(STDIN_FILENO, TCIFLUSH);

        tcsetattr(STDIN_FILENO, TCSANOW, &t);
    } else {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        tcflush(STDIN_FILENO, TCIFLUSH);
    }
#elif defined(_win_)
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    FlushConsoleInputBuffer(GetStdHandle(STD_INPUT_HANDLE));
#endif
}

} // namespace


std::optional<size_t> RunFtxuiMenu(const TString& title, const std::vector<TString>& options) {
    if (options.empty()) {
        return std::nullopt;
    }

    std::vector<std::string> labels;
    labels.reserve(options.size());
    for (const auto& opt : options) {
        labels.emplace_back(opt.c_str());
    }

    int selected = 0;
    std::optional<size_t> result;
    try {
        auto screen = ftxui::ScreenInteractive::FitComponent();
        auto menu = ftxui::Menu(&labels, &selected);
        auto exit = screen.ExitLoopClosure();

        ftxui::Component component = ftxui::CatchEvent(menu, [&, exit](const ftxui::Event& event) mutable {
            if (event == ftxui::Event::Return) {
                result = static_cast<size_t>(selected);
                exit();
                return true;
            }
            if (event == ftxui::Event::Escape || event == ftxui::Event::CtrlC) {
                exit();
                return true;
            }
            return false;
        });

        component = ftxui::Renderer(component, [title, menu] {
            return ftxui::vbox({
                ftxui::text(std::string(title)) | ftxui::bold,
                ftxui::separator(),
                menu->Render(),
                ftxui::separator(),
                ftxui::text("Use arrows to choose, Enter to confirm, Esc to cancel"),
            }) | ftxui::border | ftxui::center;
        });

        screen.Loop(component);
    } catch (const std::exception& e) {
        Cerr << "FTXUI menu failed: " << e.what() << Endl;
        return std::nullopt;
    }

    FlushStdin();
    return result;
}

bool RunFtxuiMenuWithActions(const TString& title, const std::vector<TMenuEntry>& options) {
    std::vector<TString> labels;
    labels.reserve(options.size());
    for (const auto& [label, _] : options) {
        labels.push_back(label);
    }

    if (auto idx = RunFtxuiMenu(title, labels)) {
        options[*idx].second();
        return true;
    }
    return false;
}

std::optional<TString> RunFtxuiInput(const TString& title, const TString& initial, const std::function<bool(const TString&, TString&)>& validator) {
    std::string value = std::string(initial);
    std::string error;
    std::optional<TString> result;

    try {
        auto screen = ftxui::ScreenInteractive::FitComponent();
        screen.TrackMouse(false);
        auto input = ftxui::Input(&value, "");
        auto exit = screen.ExitLoopClosure();

        ftxui::Component component = ftxui::CatchEvent(input, [&, exit](const ftxui::Event& event) mutable {
            if (event == ftxui::Event::Return) {
                TString errorMessage;
                if (validator(TString(value), errorMessage)) {
                    result = TString(value);
                    exit();
                    return true;
                }
                error = std::string(errorMessage);
                return true;
            }
            if (event == ftxui::Event::Escape || event == ftxui::Event::CtrlC) {
                exit();
                return true;
            }
            return false;
        });

        component = ftxui::Renderer(component, [&] {
            return ftxui::vbox({
                ftxui::text(std::string(title)) | ftxui::bold,
                ftxui::separator(),
                ftxui::hbox({ftxui::text("> "), input->Render()}),
                error.empty() ? ftxui::text("") : ftxui::text(error) | ftxui::color(ftxui::Color::Red),
                ftxui::separator(),
                ftxui::text("Enter to confirm, Esc to cancel"),
            }) | ftxui::border | ftxui::center;
        });

        screen.Loop(component);
    } catch (const std::exception& e) {
        Cerr << "FTXUI input failed: " << e.what() << Endl;
        return std::nullopt;
    }

    FlushStdin();
    return result;
}

bool AskYesNoFtxui(const TString& question, bool defaultAnswer) {
    std::vector<TMenuEntry> options;
    options.reserve(2);
    options.emplace_back(TStringBuilder() << "Yes" << (defaultAnswer ? " (default)" : ""), []() {});
    options.emplace_back(TStringBuilder() << "No" << (!defaultAnswer ? " (default)" : ""), []() {});

    auto idx = RunFtxuiMenu(question, {options[0].first, options[1].first});
    if (!idx) {
        return defaultAnswer;
    }
    return *idx == 0;
}

} // namespace NYdb::NConsoleClient

