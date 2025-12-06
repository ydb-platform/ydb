#include "ai_session_runner.h"
#include "session_runner_common.h"

#include <ydb/core/base/validation.h>

namespace NYdb::NConsoleClient {

namespace NAi {

namespace {

class TAiSessionRunner final : public TSessionRunnerBase {
    using TBase = TSessionRunnerBase;

public:
    TAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log)
        : TBase(CreateSessionSettings(settings), log)
        , ProfileName(settings.ProfileName)
        , ConfigurationManager(settings.ConfigurationManager)
    {}

    bool Setup(ILineReaderController::TPtr controller) final {
        AiModel = ConfigurationManager->InitAiModelProfile();
        if (!AiModel) {
            Cout << Endl << "AI profile is not set, returning to YQL interactive mode" << Endl;
            return false;
        }

        Settings.Prompt = CreatePromptPrefix(ProfileName, AiModel->GetName());
        return TBase::Setup(std::move(controller));
    }

    void HandleLine(const TString& line) final {
        Y_DEBUG_VERIFY(ConfigurationManager, "Can not handle input while ConfigurationManager is not initialized");

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

    static TString CreatePromptPrefix(const TString& profileName, const TString& aiProfile) {
        return TStringBuilder()
            << Colors.LightGreen() << (profileName ? profileName : "ydb") << Colors.OldColor() << ":"
            << Colors.Cyan() << (aiProfile ? aiProfile : "ai") << Colors.OldColor() << "> ";
    }

    static TSessionSettings CreateSessionSettings(const TAiSessionSettings& settings) {
        return {
            .Prompt = CreatePromptPrefix(settings.ProfileName, ""),
            .HistoryFilePath = TFsPath(settings.YdbPath) / "bin" / "interactive_cli_ai_history.txt",
            .HelpMessage = CreateHelpMessage(),
            .EnableYqlCompletion = false,
        };
    }

private:
    const TString ProfileName;
    const TInteractiveConfigurationManager::TPtr ConfigurationManager;
    TInteractiveConfigurationManager::TAiProfile::TPtr AiModel;
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<NAi::TAiSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
