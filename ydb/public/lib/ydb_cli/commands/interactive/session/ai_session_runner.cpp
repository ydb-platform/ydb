#include "ai_session_runner.h"
#include "session_runner_common.h"

namespace NYdb::NConsoleClient {

namespace NAi {

namespace {

class TAiSessionRunner final : public TSessionRunnerBase {
    using TBase = TSessionRunnerBase;

public:
    TAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log)
        : TBase(CreateSessionSettings(settings), log)
    {}

    void HandleLine(const TString& line) final {
        Cout << "Echo: " << line << Endl;
    }

private:
    static TString CreateHelpMessage() {
        return TStringBuilder() << Endl << "YDB CLI AI Interactive Mode – Hotkeys." << Endl
            << Endl << PrintBold("Hotkeys:") << Endl
            << "  " << PrintBold("Ctrl+T") << ": switch to basic YQL interactive mode." << Endl
            << PrintCommonHotKeys()
            << Endl << "All input is sent to the AI API. The AI will respond with YQL queries or answers to your questions." << Endl;
    }

    static TSessionSettings CreateSessionSettings(const TAiSessionSettings& settings) {
        return {
            .Prompt = TStringBuilder() << Colors.LightGreen() << (settings.ProfileName ? settings.ProfileName : "ydb") << Colors.OldColor() << ":" << Colors.Cyan() << "ai" << Colors.OldColor() << "> ",
            .HistoryFilePath = TFsPath(settings.YdbPath) / "bin" / "interactive_cli_ai_history.txt",
            .HelpMessage = CreateHelpMessage(),
            .EnableYqlCompletion = false,
        };
    }
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<NAi::TAiSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
