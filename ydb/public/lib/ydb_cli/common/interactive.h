#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/scheme/scheme.h>

namespace NYdb::NConsoleClient {

bool AskInputWithPrompt(const TString& prompt, std::function<bool(const TString&)> handler, bool verbose = false, bool exitOnError = true);

bool AskAnyInputWithPrompt(const TString& prompt, std::function<void(const TString&)> handler, bool verbose = false, bool exitOnError = true);

TString AskAnyInputWithPrompt(const TString& prompt, bool verbose = false);

bool AskYesOrNo(const TString& query, std::optional<bool> defaultAnswer = std::nullopt);

bool IsStdinInteractive();

bool IsStdoutInteractive();

bool IsStderrInteractive();

std::optional<size_t> GetTerminalWidth();
std::optional<size_t> GetErrTerminalWidth();

class TNumericOptionsPicker {
public:
    using TPickableAction = std::function<void()>;
    using TInputAction = std::function<void(const TString&)>;
    using TValidationAction = std::function<bool(const TString&)>;

    explicit TNumericOptionsPicker(bool verbose);

    void AddOption(const TString& description, TPickableAction&& action);

    void AddInputOption(const TString& description, const TString& prompt, TInputAction&& action, bool exitOnError = true);

    void AddInputOptionWithValidation(const TString& description, const TString& prompt, TValidationAction&& action, bool exitOnError = true);

    bool PickOptionAndDoAction(bool exitOnError = true) const;

private:
    std::optional<size_t> MakeNumericChoice(size_t optCount, bool exitOnError) const;

private:
    const bool Verbose = false;
    size_t OptionsCount = 0;
    std::unordered_map<size_t, TPickableAction> Options;
};

} // namespace NYdb::NConsoleClient
