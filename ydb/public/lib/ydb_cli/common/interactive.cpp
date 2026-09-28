#include "interactive.h"
#include "colors.h"

#include <contrib/restricted/patched/replxx/include/replxx.hxx>

#include <ydb/public/lib/ydb_cli/common/colors.h>

#include <util/string/cast.h>
#include <util/string/builder.h>

#if defined(_unix_)
#include <sys/ioctl.h>
#include <termios.h>

#elif defined(_win_)
#include <windows.h>
#include <io.h>
#endif

namespace NYdb::NConsoleClient {

namespace {

bool AskInputWithPrompt(const TString& prompt, std::function<bool(const TString&)> handler, bool verbose) {
    const auto& colors = NConsoleClient::AutoColors(Cout);
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
            return false;
        }

        if (handler(input)) {
            return true;
        }
    }
}

} // anonymous namespace

bool AskYesOrNo(const TString& query, bool defaultAnswer) {
    const std::vector<TString> choices = {"y", "yes", "n", "no", ""};
    bool result = defaultAnswer;
    TString prompt = TStringBuilder() << query << (defaultAnswer ? " [Y/n] " : " [y/N] ");
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
    }, /* verbose */ false);

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

bool IsStderrInteractive() {
#if defined(_win32_)
    return _isatty(_fileno(stderr));
#elif defined(_unix_)
    return isatty(fileno(stderr));
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

std::optional<size_t> GetErrTerminalWidth() {
    if (!IsStderrInteractive())
        return {};

#if defined(_win32_)
    CONSOLE_SCREEN_BUFFER_INFO screen_buf_info;
    if (GetConsoleScreenBufferInfo(GetStdHandle(STD_ERROR_HANDLE), &screen_buf_info)) {
        return screen_buf_info.srWindow.Right - screen_buf_info.srWindow.Left + 1;
    }
#elif defined(_unix_)
    struct winsize size;
    if (ioctl(STDERR_FILENO, TIOCGWINSZ, &size) != -1) {
        return size.ws_col;
    }
#endif
    return {};
}

} // namespace NYdb::NConsoleClient
