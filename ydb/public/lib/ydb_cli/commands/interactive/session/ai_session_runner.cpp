#include "ai_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/ai_model_handler.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>
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
        , ConnectionString(settings.ConnectionString)
    {
        Y_VALIDATE(ConfigurationManager, "ConfigurationManager is not initialized");
    }

    ILineReader::TPtr Setup() final {
        ModelHandler.reset();

        AiModel = ConfigurationManager->GetAiProfile(ConfigurationManager->GetActiveAiProfileName());
        if (!AiModel) {
            Cout << "Welcome to AI interactive mode, please select AI model to continue. Type /config to setup AI mode by default." << Endl;
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
                ModelHandler = TModelHandler({.Profile = AiModel, .Prompt = Settings.Prompt, .Database = Database, .Driver = Driver, .ConnectionString = ConnectionString}, Log);
            } catch (const std::exception& e) {
                ModelHandler = std::nullopt;
                Cerr << Colors.Red() << "Failed to setup AI model session: " << e.what() << Colors.OldColor() << Endl;
                return;
            }
        }

        if (to_lower(line) == "/help") {
            Cout << Endl;
            PrintFtxuiMessage(CreateHelpMessage(), "YDB CLI AI Interactive Mode – Hotkeys", ftxui::Color::White);
            Cout << Endl;
            return;
        }

        if (to_lower(line) == "/model") {
            SwitchAiProfile();
            return;
        }

        if (to_lower(line) == "/config") {
            ChangeSessionSettings();
            return;
        }

        Cout << Endl;

        std::shared_ptr<TProgressWaiterBase> spinner;
        double lastThinkingTime = 0.0;
        auto onStart = [&spinner]() {
            spinner = std::make_shared<TStaticProgressWaiter>("Agent is thinking...");
        };
        auto onFinish = [&spinner, &lastThinkingTime]() {
            if (spinner) {
                lastThinkingTime = spinner->Stop(true).SecondsFloat();
                spinner.reset();
            }
        };

        ModelHandler->HandleLine(line, onStart, onFinish, [&lastThinkingTime](){ return lastThinkingTime; });
    }

private:
    static ftxui::Element CreateHelpMessage() {
        using namespace ftxui;

        std::vector<ftxui::Element> elements = {
            paragraph("All input is sent to the AI API. The AI will respond with YQL queries or answers to your questions."),
            text(""),
            CreateEntityName("Hotkeys:"),
            CreateListItem(hbox({
                CreateEntityName("Ctrl+T"), text(" or "), CreateEntityName("/switch"), 
                text(": switch to "), 
                text(ToString(TInteractiveConfigurationManager::EMode::YQL)) | color(Color::Green), 
                text(" interactive mode.")
            })),
        };

        PrintCommonHotKeys(elements);

        elements.emplace_back(text(""));
        elements.emplace_back(CreateEntityName("Interactive Commands:"));
        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("/model"), text(": switch AI mode or setup new one.")
        })));
        elements.emplace_back(CreateListItem(hbox({
            CreateEntityName("/config"), text(": change AI mode settings, e. g. change AI model or clear model context.")
        })));

        PrintCommonInteractiveCommands(elements);

        return vbox(elements);
    }

    static TLineReaderSettings CreateSessionSettings(const TAiSessionSettings& settings) {
        return {
            .Driver = settings.Driver,
            .Database = settings.Database,
            .Prompt = TStringBuilder() << TInteractiveConfigurationManager::ModeToString(TInteractiveConfigurationManager::EMode::AI) << "> ",
            .HistoryFilePath = TFsPath(settings.YdbPath) / "bin" / "interactive_cli_ai_history.txt",
            .AdditionalCommands = {"/help", "/model", "/config"},
            .Placeholder = "Type message (Enter to send, Ctrl+J for newline, Ctrl+T for YQL mode, Ctrl+D to exit)",
            .EnableYqlCompletion = false,
        };
    }

    void ChangeSessionSettings() {
        Cout << Endl;

        for (bool exit = false; !exit;) {
            std::vector<TMenuEntry> options;

            const auto& profile = ConfigurationManager->GetActiveAiProfileName();
            const auto& profiles = ConfigurationManager->ListAiProfiles();
            if (const auto it = profiles.find(profile); profile && it != profiles.end()) {
                options.emplace_back(TStringBuilder() << "Change current AI model \"" << profile << "\" settings", [profile = it->second, &exit, this]() {
                    Cout << Endl << "Changing current AI model \"" << profile->GetName() << "\" settings." << Endl << Endl;
                    if (!profile->SetupProfile()) {
                        exit = true;
                        return;
                    }

                    ChangeAiProfile(profile);
                });
            }

            options.emplace_back("Switch AI model", [&]() {
                if (!SwitchAiProfile()) {
                    exit = true;
                }
            });

            options.push_back({"Clear session context", [&]() {
                Cout << Endl << "Session context cleared." << Endl << Endl;
                if (ModelHandler) {
                    ModelHandler->ClearContext();
                }
                exit = true;
            }});

            switch (ConfigurationManager->GetDefaultMode()) {
                case TInteractiveConfigurationManager::EMode::YQL:
                    options.push_back({"Set AI interactive mode by default", [&]() {
                        Cout << Endl << "Setting AI interactive mode by default." << Endl << Endl;
                        ConfigurationManager->ChangeDefaultMode(TInteractiveConfigurationManager::EMode::AI);
                        exit = true;
                    }});
                    break;
                case TInteractiveConfigurationManager::EMode::AI:
                    options.push_back({"Set YQL interactive mode by default", [&]() {
                        Cout << Endl << "Setting YQL interactive mode by default." << Endl << Endl;
                        ConfigurationManager->ChangeDefaultMode(TInteractiveConfigurationManager::EMode::YQL);
                        exit = true;
                    }});
                    break;
                case TInteractiveConfigurationManager::EMode::Invalid:
                    Y_VALIDATE(false, "Invalid default mode: " << ConfigurationManager->GetDefaultMode());
            }

            if (!RunFtxuiMenuWithActions("Please choose AI session setting to change:", options)) {
                exit = true;
                Cout << Endl;
            }
        }
    }

    bool SwitchAiProfile() {
        Cout << Endl;
        auto newProfile = ConfigurationManager->SelectAiModelProfile();

        if (newProfile) {
            ChangeAiProfile(std::move(newProfile));
            return true;
        }

        return false;
    }

    void ChangeAiProfile(TInteractiveConfigurationManager::TAiProfile::TPtr profile) {
        Y_VALIDATE(profile, "Profile is not set");
        Y_VALIDATE(AiModel, "AI session is not initialized");

        const auto& newProfileName = profile->GetName();
        if (ModelHandler) {
            Cout << Colors.Yellow() << "Active AI profile is changed"
                << (newProfileName != AiModel->GetName() ? TStringBuilder() << " to \"" << newProfileName << "\"" : TStringBuilder())
                << ", session context will be reset" << Colors.OldColor() << Endl << Endl;
        } else if (newProfileName != AiModel->GetName()) {
            Cout << "Switching AI profile to \"" << newProfileName << "\"" << Endl << Endl;
        }

        ConfigurationManager->ChangeActiveAiProfile(newProfileName);
        AiModel = profile;
        ModelHandler.reset();
    }

private:
    const TInteractiveConfigurationManager::TPtr ConfigurationManager;
    const TString Database;
    const TDriver Driver;
    const TString ConnectionString;

    TInteractiveConfigurationManager::TAiProfile::TPtr AiModel;
    std::optional<TModelHandler> ModelHandler;
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<NAi::TAiSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
