#include "ai_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/ai_model_handler.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log_defs.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

namespace NYdb::NConsoleClient {

namespace NAi {

namespace {

class TAiSessionRunner final : public TSessionRunnerBase {
    using TBase = TSessionRunnerBase;

public:
    TAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log)
        : TBase(CreateSessionSettings(settings), log)
        , ConfigurationManager(settings.ConfigurationManager)
        , Database(settings.Database)
        , Driver(settings.Driver)
    {
        Y_VALIDATE(ConfigurationManager, "ConfigurationManager is not initialized");
    }

    ILineReader::TPtr Setup() final {
        ModelHandler.reset();

        AiModel = ConfigurationManager->GetAiProfile(ConfigurationManager->GetActiveAiProfileName());
        if (!AiModel) {
            AiModel = ConfigurationManager->SelectAiModelProfile();
        }

        if (!AiModel) {
            Cout << Endl << "AI profile is not set, returning to YQL interactive mode" << Endl;
            return nullptr;
        }

        TString validationError;
        Y_VALIDATE(AiModel->IsValid(validationError), "AI profile is not valid: " << validationError);
        return TBase::Setup();
    }

    void HandleLine(const TString& line) final {
        Y_VALIDATE(AiModel, "Can not handle input while AiModel is not initialized");
        Y_VALIDATE(ConfigurationManager->GetActiveAiProfileName() == AiModel->GetName(), "Unexpected active AI profile");

        if (!ModelHandler) {
            try {
                ModelHandler = TModelHandler({.Profile = AiModel, .Prompt = Settings.Prompt, .Database = Database, .Driver = Driver}, Log);
            } catch (const std::exception& e) {
                ModelHandler = std::nullopt;
                Cerr << Colors.Red() << "Failed to setup AI model session:: " << e.what() << Colors.OldColor() << Endl;
                return;
            }
        }

        if (to_lower(line) == "/model") {
            SwitchAiProfile();
            return;
        }

        if (to_lower(line) == "/config") {
            ChangeSessionSettings();
            return;
        }

        ModelHandler->HandleLine(line);
    }

private:
    static TString CreateHelpMessage() {
        using TLog = TInteractiveLogger;

        return TStringBuilder() << Endl << "YDB CLI AI Interactive Mode – Hotkeys." << Endl
            << Endl << TLog::EntityName("Hotkeys:") << Endl
            << "  " << TLog::EntityName("Ctrl+I") << " or " << TLog::EntityName("/switch") << ": switch to basic " << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::YQL) << " interactive mode." << Endl
            << PrintCommonHotKeys()
            << Endl << TLog::EntityName("Interactive Commands:") << Endl
            << "  " << TLog::EntityName("/model") << ": switch AI mode or setup new one." << Endl
            << "  " << TLog::EntityName("/config") << ": change AI mode settings, e. g. change AI model or clear model context." << Endl
            << "  " << TLog::EntityName("/help") << ": print this help message." << Endl
            << Endl << "All input is sent to the AI API. The AI will respond with YQL queries or answers to your questions." << Endl;
    }

    static TLineReaderSettings CreateSessionSettings(const TAiSessionSettings& settings) {
        return {
            .Driver = settings.Driver,
            .Database = settings.Database,
            .Prompt = TStringBuilder() << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::AI) << "> ",
            .HistoryFilePath = TFsPath(settings.YdbPath) / "bin" / "interactive_cli_ai_history.txt",
            .HelpMessage = CreateHelpMessage(),
            .AdditionalCommands = {"/model", "/config"},
            .EnableYqlCompletion = false,
        };
    }

    void ChangeSessionSettings() {
        for (bool exit = false; !exit;) {
            std::vector<TMenuEntry> options;

            options.push_back({"Clear session context", [&]() {
                Cout << "Session context cleared." << Endl;
                if (ModelHandler) {
                    ModelHandler->ClearContext();
                }
            }});

            TString currentProfile;
            if (const auto& profile = ConfigurationManager->GetActiveAiProfileName()) {
                currentProfile = TStringBuilder() << " (current profile: \"" << profile << "\")";
            }
            options.push_back({TStringBuilder() << "Change AI model settings" << currentProfile, [&]() {
                ChangeProfileSettings();
            }});

            switch (ConfigurationManager->GetDefaultMode()) {
                case TInteractiveConfigurationManager::EMode::YQL:
                    options.push_back({"Set AI interactive mode by default", [config = ConfigurationManager]() {
                        Cout << "Setting AI interactive mode by default." << Endl;
                        config->ChangeDefaultMode(TInteractiveConfigurationManager::EMode::AI);
                    }});
                    break;
                case TInteractiveConfigurationManager::EMode::AI:
                    options.push_back({"Set YQL interactive mode by default", [config = ConfigurationManager]() {
                        Cout << "Setting YQL interactive mode by default." << Endl;
                        config->ChangeDefaultMode(TInteractiveConfigurationManager::EMode::YQL);
                    }});
                    break;
                case TInteractiveConfigurationManager::EMode::Invalid:
                    Y_VALIDATE(false, "Invalid default mode: " << ConfigurationManager->GetDefaultMode());
            }

            options.push_back({"Don't do anything, just exit", [&]() { exit = true; }});

            if (!RunFtxuiMenuWithActions("Please choose AI session setting to change:", options)) {
                exit = true;
            }
        }

        Cout << Endl;
    }

    void ChangeProfileSettings() {
        for (bool exit = false; !exit;) {
            std::vector<TMenuEntry> options;

            const auto& profile = ConfigurationManager->GetActiveAiProfileName();
            const auto& profiles = ConfigurationManager->ListAiProfiles();
            if (const auto it = profiles.find(profile); profile && it != profiles.end()) {
                options.emplace_back(TStringBuilder() << "Change current AI model \"" << profile << "\" settings", [profile = it->second, this]() {
                    Cout << Endl << "Changing current AI model \"" << profile->GetName() << "\" settings." << Endl;
                    profile->SetupProfile();
                    ChangeAiProfile(profile);
                });
            }

            options.emplace_back("Switch AI model", [&]() { SwitchAiProfile(); });
            options.push_back({"Don't do anything, just exit", [&]() { exit = true; }});

            if (!RunFtxuiMenuWithActions("Please choose desired action with AI model:", options)) {
                exit = true;
            }
        }
    }

    void SwitchAiProfile() {
        if (auto newProfile = ConfigurationManager->SelectAiModelProfile()) {
            ChangeAiProfile(std::move(newProfile));
        }
    }

    void ChangeAiProfile(TInteractiveConfigurationManager::TAiProfile::TPtr profile) {
        Y_VALIDATE(profile, "Profile is not set");
        Y_VALIDATE(AiModel, "AI session is not initialized");

        const auto& newProfileName = profile->GetName();
        if (ModelHandler) {
            Cout << Colors.Yellow() << "Active AI profile is changed"
                << (newProfileName != AiModel->GetName() ? TStringBuilder() << " to \"" << newProfileName << "\"" : TStringBuilder())
                << ", session context will be reset" << Colors.OldColor();
        } else if (newProfileName != AiModel->GetName()) {
            Cout << "Switching AI profile to \"" << newProfileName << "\"";
        }

        ConfigurationManager->ChangeActiveAiProfile(newProfileName);
        AiModel = profile;
        ModelHandler.reset();
    }

private:
    const TInteractiveConfigurationManager::TPtr ConfigurationManager;
    const TString Database;
    const TDriver Driver;

    TInteractiveConfigurationManager::TAiProfile::TPtr AiModel;
    std::optional<TModelHandler> ModelHandler;
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<NAi::TAiSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
