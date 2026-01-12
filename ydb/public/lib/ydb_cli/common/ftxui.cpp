#include "ftxui.h"
#include "colors.h"

#include <ydb/library/yverify_stream/yverify_stream.h>

#include <contrib/libs/ftxui/include/ftxui/component/component.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/event.hpp>
#include <contrib/libs/ftxui/include/ftxui/component/screen_interactive.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/elements.hpp>
#include <contrib/libs/ftxui/include/ftxui/dom/node.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/color.hpp>
#include <contrib/libs/ftxui/include/ftxui/screen/screen.hpp>

#include <library/cpp/colorizer/colors.h>

#include <util/charset/utf8.h>
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

class TFtxuiMenuRunner {
    static constexpr int MENU_EXTRA_HEIGHT = 8; // Header + footer + border

public:
    TFtxuiMenuRunner(const TString& title, const std::vector<TString>& options, size_t maxPageSize)
        : Title(title)
        , Options(options)
        , PageSize(CalculatePageSize(std::min(options.size(), maxPageSize)))
        , PageCount((std::max(static_cast<int>(Options.size()), 1) - 1) / PageSize + 1)
        , Screen(ftxui::ScreenInteractive::FitComponent())
        , Exit(Screen.ExitLoopClosure())
    {
        Y_VALIDATE(PageSize > 0, "PageSize must be greater than 0");
        Y_VALIDATE(!Options.empty() && Options.size() <= std::numeric_limits<int>::max(), "Unexpected Options size: " << Options.size());

        Screen.TrackMouse(false);

        SetupVisibleOptions();
        Menu = ftxui::Menu(&VisibleOptions, &Selected);

        auto component = ftxui::CatchEvent(Menu, [this](const ftxui::Event& event) { return CatchEventHandle(event); });

        Renderer = ftxui::Renderer(component, [this]() { return Render(); });
    }

    std::optional<size_t> Run() {
        if (PageCount > 1) {
            // Clear screen first, because menu can be redrawn
            Cout << "\033[2J\033[H" << Flush;
        }

        Screen.Loop(Renderer);
        return Result;
    }

private:
    static int CalculatePageSize(size_t maxPageSize) {
        Y_VALIDATE(0 < maxPageSize && maxPageSize < std::numeric_limits<int>::max(), "Unexpected MaxPageSize: " << maxPageSize);

        const auto h = ftxui::Terminal::Size().dimy;
        return std::min(h > MENU_EXTRA_HEIGHT ? h - MENU_EXTRA_HEIGHT : 1, static_cast<int>(maxPageSize));
    }

    void SetupVisibleOptions() {
        Y_VALIDATE(PageBegin >= 0 && PageSize > 0, "Unexpected PageBegin: " << PageBegin << " or PageSize: " << PageSize);

        const size_t pageEnd = PageBegin + PageSize;
        Y_VALIDATE(pageEnd <= Options.size(), "Unexpected PageBegin: " << PageBegin << ", number of options: " << Options.size() << ", PageSize: " << PageSize);

        VisibleOptions.assign(Options.begin() + PageBegin, Options.begin() + pageEnd);
    }

    void MoveSelected(int delta, bool allowJump) {
        Y_VALIDATE(!Options.empty(), "Options must not be empty");

        int newRowIdx = PageBegin + Selected + delta;
        if (allowJump) {
            const int totalSize = Options.size();
            newRowIdx = (newRowIdx % totalSize + totalSize) % totalSize;
        } else {
            newRowIdx = std::min(std::max(newRowIdx, 0), static_cast<int>(Options.size() - 1));
        }
        Y_VALIDATE(newRowIdx >= 0 && newRowIdx < static_cast<int>(Options.size()), "Unexpected newRowIdx: " << newRowIdx);

        if (newRowIdx <= PageBegin) {
            // Move page up
            PageBegin = std::max(newRowIdx - 1, 0);
        } else if (newRowIdx + 1 >= PageBegin + PageSize) {
            // Move page down
            PageBegin = std::min(newRowIdx - PageSize + 2, std::min(newRowIdx, static_cast<int>(Options.size()) - PageSize));
        }

        Selected = newRowIdx - PageBegin;
        SetupVisibleOptions();

        if (Options.size() > 1) {
            // Preserve row highlighting
            if (Selected <= 0) {
                Selected++;
                Screen.PostEvent(ftxui::Event::ArrowUp);
            } else {
                Selected--;
                Screen.PostEvent(ftxui::Event::ArrowDown);
            }
        }
    }

    bool CatchEventHandle(const ftxui::Event& event) {
        Y_VALIDATE(Selected >= 0, "Unexpected selected value: " << Selected);
        const auto rowIdx = PageBegin + Selected;

        if (event == ftxui::Event::Return) {
            Result = rowIdx;
            Exit();
            return true;
        }

        if (event == ftxui::Event::Escape || event == ftxui::Event::CtrlC) {
            Exit();
            return true;
        }

        if (event == ftxui::Event::PageDown || event == ftxui::Event::ArrowRight) {
            if (rowIdx + 1 < static_cast<int>(Options.size())) {
                MoveSelected(PageSize, /* allowJump */ false);
            }
            return true;
        }

        if (event == ftxui::Event::PageUp || event == ftxui::Event::ArrowLeft) {
            if (rowIdx > 0) {
                MoveSelected(-PageSize, /* allowJump */ false);
            }
            return true;
        }

        const bool atEndOfPage = Selected + 2 >= PageSize;
        const bool isLastPage = PageBegin + PageSize == static_cast<int>(Options.size());
        const bool jumpToTop = PageBegin + Selected + 1 == static_cast<int>(Options.size()) && Options.size() > 1;
        if (event == ftxui::Event::ArrowDown && atEndOfPage && (!isLastPage || jumpToTop)) {
            MoveSelected(1, /* allowJump */ true);
            return true;
        }

        const bool atBeginOfPage = Selected < 2;
        const bool isFirstPage = PageBegin == 0;
        const bool jumpToBottom = PageBegin + Selected == 0 && Options.size() > 1;
        if (event == ftxui::Event::ArrowUp && atBeginOfPage && (!isFirstPage || jumpToBottom)) {
            MoveSelected(-1, /* allowJump */ true);
            return true;
        }

        return false;
    }

    ftxui::Element Render() const {
        std::vector<ftxui::Element> elements = {ftxui::text(std::string(Title)) | ftxui::bold};

        if (PageCount > 1) {
            std::string pageInfo = TStringBuilder() << "Page " << (PageBegin + Selected) / PageSize + 1 << "/" << PageCount;
            elements.emplace_back(ftxui::text(pageInfo) | ftxui::bold);
        }

        elements.emplace_back(ftxui::separator());
        elements.emplace_back(Menu->Render());
        elements.emplace_back(ftxui::separator());

        auto info = ftxui::text("Use arrows ↑ and ↓ to choose, Enter to confirm, Esc to cancel");
        if (PageCount > 1) {
            elements.emplace_back(ftxui::vbox({
                std::move(info),
                ftxui::text("Use arrows ← and → to navigate through pages")
            }));
        } else {
            elements.emplace_back(std::move(info));
        }

        return ftxui::vbox(elements) | ftxui::border | ftxui::center;
    }

private:
    const TString Title;
    const std::vector<TString> Options;
    const int PageSize = 1;
    const int PageCount = 1;

    int Selected = 0; // Index of the selected option inside the current page over `VisibleOptions`
    int PageBegin = 0; // Index of the first option in the current page over `Options`
    std::vector<std::string> VisibleOptions;
    std::optional<size_t> Result;

    ftxui::Component Menu;
    ftxui::Component Renderer;
    ftxui::ScreenInteractive Screen;
    ftxui::Closure Exit;
};

} // anonymous namespace

std::optional<size_t> RunFtxuiMenu(const TString& title, const std::vector<TString>& options, size_t maxPageSize) {
    if (options.empty()) {
        return std::nullopt;
    }

    std::optional<size_t> result;
    try {
        result = TFtxuiMenuRunner(title, options, maxPageSize).Run();
    } catch (const std::exception& e) {
        const auto& colors = NConsoleClient::AutoColors(Cerr);
        Cerr << colors.Yellow() << "FTXUI menu failed: " << e.what() << colors.OldColor() << Endl;
    }

    FlushStdin();
    return result;
}

bool RunFtxuiMenuWithActions(const TString& title, const std::vector<TMenuEntry>& options, size_t maxPageSize) {
    std::vector<TString> labels;
    labels.reserve(options.size());
    for (const auto& [label, _] : options) {
        labels.push_back(label);
    }

    if (auto idx = RunFtxuiMenu(title, labels, maxPageSize)) {
        Y_VALIDATE(*idx < options.size(), "Unexpected option index: " << *idx);
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

    auto idx = RunFtxuiMenu(question, {options[0].first, options[1].first}, /* maxPageSize */ 2);
    if (!idx) {
        return defaultAnswer;
    }
    return *idx == 0;
}

void PrintFtxuiMessage(std::optional<ftxui::Element> message, const TString& title, ftxui::Color color) {
    int titleUtf8Size = 0;
    for (size_t i = 0; i < title.size(); i += UTF8RuneLen(title[i])) {
        titleUtf8Size++;
    }

    const auto screenWidth = ftxui::Terminal::Size().dimx;
    std::string separator;
    separator.reserve(screenWidth);
    for (int i = 0; i < screenWidth - titleUtf8Size - 4; ++i) {
        separator += "─";
    }

    std::vector<ftxui::Element> elements = {
        ftxui::hbox({
            ftxui::text("──"),
            ftxui::text(" " + std::string(title) + " ") | ftxui::bold,
            ftxui::text(separator),
        }) | ftxui::color(color),
    };

    if (message) {
        elements.push_back(*message);
        elements.push_back(ftxui::separator() | ftxui::color(color));
    }

    auto document = ftxui::vbox(elements) | ftxui::bgcolor(ftxui::Color::Grey11);
    auto screen = ftxui::Screen::Create(ftxui::Dimension::Full(), ftxui::Dimension::Fit(document));
    ftxui::Render(screen, document);

    screen.Print();
    Cout << Endl;
}

void PrintFtxuiMessage(const TString& body, const TString& title, ftxui::Color color) {
    std::optional<ftxui::Element> message;
    if (body) {
        message = ftxui::paragraph(std::string(body)) | ftxui::color(ftxui::Color::White);
    }
    PrintFtxuiMessage(message, title, color);
}

} // namespace NYdb::NConsoleClient


