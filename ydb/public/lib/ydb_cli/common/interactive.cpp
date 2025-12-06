#include "interactive.h"

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <library/cpp/colorizer/colors.h>

#include <util/string/builder.h>

#if defined(_unix_)
#include <sys/ioctl.h>
#include <termios.h>

#elif defined(_win_)
#include <windows.h>
#include <io.h>
#endif

namespace NYdb::NConsoleClient {

void AskInputWithPrompt(const TString& prompt, std::function<bool(const TString&)> handler, bool verbose) {
    const auto& colors = NColorizer::AutoColors(Cout);
    replxx::Replxx rx;

    while (true) {
        const char* input = nullptr;

        try {
            input = rx.input(prompt.c_str());
        } catch (const std::exception& e) {
            if (verbose) {
                Cerr << colors.Yellow() << "Error while reading input: " << colors.OldColor() << e.what() << Endl;
            }
        }

        if (!input) {
            Cerr << "Exiting." << Endl;
            exit(1);
        }

        if (handler(input)) {
            break;
        }
    }
}

void AskAnyInputWithPrompt(const TString& prompt, std::function<void(const TString&)> handler, bool verbose) {
    AskInputWithPrompt(prompt, [&](const TString& input) {
        handler(input);
        return true;
    }, verbose);
}

TString AskAnyInputWithPrompt(const TString& prompt, bool verbose) {
    TString result;
    AskAnyInputWithPrompt(prompt, [&](const TString& input) {
        result = input;
    }, verbose);
    return result;
}

bool AskYesOrNo(const TString& query, std::optional<bool> defaultAnswer) {
    std::vector<TString> choices = {"y", "yes", "n", "no"};
    if (defaultAnswer) {
        choices.push_back("");
    }

    bool result = defaultAnswer.value_or(false);
    TString prompt = query;
    AskInputWithPrompt(prompt, [&](const TString& input) {
        const auto choice = to_lower(input);
        if (!IsIn(choices, choice)) {
            prompt = "Please type \"y\" (yes) or \"n\" (no): ";
            return false;
        }

        if (choice == "y" || choice == "yes") {
            result = true;
        } else if (choice == "n" || choice == "no") {
            result = false;
        }

        return true;
    });

    return result;
}

bool AskPrompt(const std::string& query, bool defaultAnswer) {
    if (!IsStdinInteractive()) {
        Cerr << query << " Non interactive session, assuming default answer: " << defaultAnswer << Endl;
        return defaultAnswer;
    }

    bool result = defaultAnswer;
    TString prompt = TStringBuilder() << query << (defaultAnswer ? " [Y/n] " : " [y/N] ");
    AskInputWithPrompt(prompt, [&](const TString& input) {
        const auto choice = to_lower(input);
        if (!IsIn({"y", "yes", "n", "no", ""}, choice)) {
            prompt = "Please type \"y\" (yes) or \"n\" (no): ";
            return false;
        }

        if (choice == "y" || choice == "yes") {
            result = true;
        } else if (choice == "n" || choice == "no") {
            result = false;
        }

        return true;
    });

    return result;
}

bool IsStdinInteractive() {
#if defined(_win32_)
    return _isatty(_fileno(stdin));
#elif defined(_unix_)
    return isatty(fileno(stdin));
#endif
    return true;
}

bool IsStdoutInteractive() {
#if defined(_win32_)
    return _isatty(_fileno(stdout));
#elif defined(_unix_)
    return isatty(fileno(stdout));
#endif
    return true;
}

std::optional<size_t> GetTerminalWidth() {
    if (!IsStdoutInteractive())
        return {};

#if defined(_win32_)
    CONSOLE_SCREEN_BUFFER_INFO screen_buf_info;
    if (GetConsoleScreenBufferInfo(GetStdHandle(STD_OUTPUT_HANDLE), &screen_buf_info)) {
        return screen_buf_info.srWindow.Right - screen_buf_info.srWindow.Left + 1;
    }
#elif defined(_unix_)
    struct winsize size;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &size) != -1) {
        return size.ws_col;
    }
#endif
    return {};
}

} // namespace NYdb::NConsoleClient
