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

bool AskInputWithPrompt(const TString& prompt, std::function<bool(const TString&)> handler, bool verbose, bool exitOnError) {
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
            if (exitOnError) {
                Cerr << "Exiting." << Endl;
                exit(1);
            } else {
                return false;
            }
        }

        if (handler(input)) {
            return true;
        }
    }
}

bool AskAnyInputWithPrompt(const TString& prompt, std::function<void(const TString&)> handler, bool verbose, bool exitOnError) {
    return AskInputWithPrompt(prompt, [&](const TString& input) {
        handler(input);
        return true;
    }, verbose, exitOnError);
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
    }, /* verbose */ false, /* exitOnError */ defaultAnswer.has_value());

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

TNumericOptionsPicker::TNumericOptionsPicker(bool verbose)
    : Verbose(verbose)
{}

void TNumericOptionsPicker::AddOption(const TString& description, TPickableAction&& action) {
    const auto& colors = NConsoleClient::AutoColors(Cout);
    Cout << " [" << colors.Green() << ++OptionsCount << colors.OldColor() << "] " << description << Endl;
    Options.emplace(OptionsCount, std::move(action));
}

void TNumericOptionsPicker::AddInputOption(const TString& description, const TString& prompt, TInputAction&& action, bool exitOnError) {
    AddOption(description, [this, prompt, a = std::move(action), exitOnError]() {
        AskAnyInputWithPrompt(prompt, a, Verbose, exitOnError);
    });
}

void TNumericOptionsPicker::AddInputOptionWithValidation(const TString& description, const TString& prompt, TValidationAction&& action, bool exitOnError) {
    AddOption(description, [this, prompt, a = std::move(action), exitOnError]() {
        AskInputWithPrompt(prompt, a, Verbose, exitOnError);
    });
}

bool TNumericOptionsPicker::PickOptionAndDoAction(bool exitOnError) const {
    while (true) {
        const auto numericChoice = MakeNumericChoice(OptionsCount, exitOnError);
        if (!numericChoice) {
            return false;
        }

        if (const auto it = Options.find(*numericChoice); it != Options.end()) {
            // Do action
            it->second();
            return true;
        }

        Cerr << "Can't find action with index " << *numericChoice << Endl;
    }
}

std::optional<size_t> TNumericOptionsPicker::MakeNumericChoice(size_t optCount, bool exitOnError) const {
    size_t numericChoice = 0;
    TString prompt = "Please enter your numeric choice: ";
    const bool result = AskInputWithPrompt(prompt, [&](const TString& input) {
        if (TryFromString(input, numericChoice) && 0 < numericChoice && numericChoice <= optCount) {
            return true;
        }

        prompt = TStringBuilder() << "Please enter a value between 1 and " << optCount << ": ";
        return false;
    }, Verbose, exitOnError);

    if (!result) {
        return std::nullopt;
    }

    return numericChoice;
}

} // namespace NYdb::NConsoleClient
