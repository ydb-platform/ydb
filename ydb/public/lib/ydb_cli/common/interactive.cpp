#include "interactive.h"

#if defined(_unix_)
#include <sys/ioctl.h>
#include <termios.h>

#elif defined(_win_)
#include <windows.h>
#include <io.h>
#endif

namespace NYdb {
namespace NConsoleClient {

bool AskYesOrNo() {
    TString input;
    for (;;) {
        Cin >> input;
        if (to_lower(input) == "y" || to_lower(input) == "yes") {
            return true;
        } else if (to_lower(input) == "n" || to_lower(input) == "no") {
            return false;
        } else {
            Cout << "Type \"y\" (yes) or \"n\" (no): ";
        }
    }
    return false;
}

bool AskPrompt(const std::string &query, bool defaultAnswer) {
    if (IsStdinInteractive()) {
        Cerr << query << (defaultAnswer ? " [Y/n] " : " [y/N] ");

        while(true) {
            std::string text;
            std::getline(std::cin, text);

            std::transform(text.begin(), text.end(), text.begin(),
                [](unsigned char c){ return std::tolower(c); });

            if (text == "y" || text == "yes") {
                return true;
            } else if (text == "n" || text == "no") {
                return false;
            } else if (text == "") {
                return defaultAnswer;
            } else {
                Cerr << "Please type \"y\" or \"n\". ";
            }
        }
    } else {
        Cerr << query << " Non interactive session, assuming default answer: " << defaultAnswer << Endl;
    }

    return defaultAnswer;
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

}
}
