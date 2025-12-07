#include "ai_session_runner.h"
#include "session_runner_common.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

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
    {
        Y_DEBUG_VERIFY(ConfigurationManager, "ConfigurationManager is not initialized");
        Settings.KeyHandlers = {{'G', [&]() { ChangeSessionSettings(); }}};
    }

    ~TAiSessionRunner() {
        Settings.KeyHandlers.clear();

        if (Controller) {
            try {
                Controller->Setup(Settings);
            } catch (...) {
                Log.Critical() << "Failed to reset line reader controller: " << CurrentExceptionMessage() << Endl;
            }
        }
    }

    bool Setup(ILineReaderController::TPtr controller) final {
        HasContext = false;
        AiModel = ConfigurationManager->InitAiModelProfile();
        if (!AiModel) {
            Cout << Endl << "AI profile is not set, returning to YQL interactive mode" << Endl;
            return false;
        }

        TString validationError;
        Y_DEBUG_VERIFY(AiModel->IsValid(validationError), "AI profile is not valid %s", validationError.c_str());

        Settings.Prompt = CreatePromptPrefix(ProfileName, AiModel->GetName());
        return TBase::Setup(std::move(controller));
    }

    void HandleLine(const TString& line) final {
        Y_DEBUG_VERIFY(AiModel, "Can not handle input while AiModel is not initialized");
        Y_DEBUG_VERIFY(ConfigurationManager->GetActiveAiProfileName() == AiModel->GetName(), "Unexpected active AI profile");
        HasContext = true;

        Cout << "Echo: " << line << Endl;
    }

private:
    static TString CreateHelpMessage() {
        return TStringBuilder() << Endl << "YDB CLI AI Interactive Mode – Hotkeys." << Endl
            << Endl << PrintBold("Hotkeys:") << Endl
            << "  " << PrintBold("Ctrl+T") << ": switch to basic YQL interactive mode." << Endl
            << "  " << PrintBold("Ctrl+G") << ": change AI session settings, e. g. change AI profile or clear model context." << Endl
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
            .Prompt = CreatePromptPrefix(settings.ProfileName, settings.ConfigurationManager->GetActiveAiProfileName()),
            .HistoryFilePath = TFsPath(settings.YdbPath) / "bin" / "interactive_cli_ai_history.txt",
            .HelpMessage = CreateHelpMessage(),
            .EnableYqlCompletion = false,
        };
    }

    void ChangeSessionSettings() {
        Cout << Endl << "Please choose AI session setting to change:" << Endl;
        TNumericOptionsPicker picker(Log.IsVerbose());

        TString currentProfile;
        if (const auto& profile = ConfigurationManager->GetActiveAiProfileName()) {
            currentProfile = TStringBuilder() << " (current profile: \"" << profile << "\")";
        }
        picker.AddOption(TStringBuilder() << "Change AI profile settings" << currentProfile, [&]() {
            ChangeProfileSettings();
        });

        switch (ConfigurationManager->GetDefaultMode()) {
            case TInteractiveConfigurationManager::EMode::YQL:
                picker.AddOption("Set AI interactive mode by default", [config = ConfigurationManager]() {
                    Cout << "Setting AI interactive mode by default." << Endl;
                    config->ChangeDefaultMode(TInteractiveConfigurationManager::EMode::AI);
                });
                break;
            case TInteractiveConfigurationManager::EMode::AI:
                picker.AddOption("Set YQL interactive mode by default", [config = ConfigurationManager]() {
                    Cout << "Setting YQL interactive mode by default." << Endl;
                    config->ChangeDefaultMode(TInteractiveConfigurationManager::EMode::YQL);
                });
                break;
            case TInteractiveConfigurationManager::EMode::Invalid:
                Y_DEBUG_VERIFY(false, "Invalid default mode: %s", ToString(ConfigurationManager->GetDefaultMode()).c_str());
        }

        picker.AddOption("Don't do anything, just exit", []() {});

        picker.PickOptionAndDoAction(/* exitOnError */ false);
        Cout << Endl;
    }

    void ChangeProfileSettings() {
        Cout << Endl << "Please choose desired action with AI profiles:" << Endl;
        TNumericOptionsPicker picker(Log.IsVerbose());

        const auto& profile = ConfigurationManager->GetActiveAiProfileName();
        const auto& profiles = ConfigurationManager->ListAiProfiles();
        if (const auto it = profiles.find(profile); profile && it != profiles.end()) {
            picker.AddOption(TStringBuilder() << "Change current AI profile \"" << profile << "\" settings", [profile = it->second]() {
                Cout << Endl << "Changing current AI profile \"" << profile->GetName() << "\" settings." << Endl;
                profile->SetupProfile();
            });
        }

        TInteractiveConfigurationManager::TAiProfile::TPtr otherProfile;
        for (const auto& [name, model] : profiles) {
            if (name != profile) {
                picker.AddOption(TStringBuilder() << "Switch AI profile to \"" << name << "\"", [model, this]() {
                    SwitchAiProfile(model);
                });

                if (!otherProfile) {
                    otherProfile = model;
                }
            }
        }

        picker.AddOption("Create new AI profile", [&]() {
            Cout << Endl << "Creating new AI profile." << Endl;
            if (const auto profile = ConfigurationManager->CreateNewAiModelProfile()) {
                SwitchAiProfile(profile);
            }
        });

        if (profile && otherProfile) {
            picker.AddOption(TStringBuilder() << "Remove current AI profile \"" << profile << "\"", [profile, otherProfile, this]() {
                Cout << "Removing current AI profile \"" << profile << "\"" << Endl;
                ConfigurationManager->RemoveAiModelProfile(profile);
                SwitchAiProfile(std::move(otherProfile));
            });
        }

        picker.AddOption("Don't do anything, just exit", []() {});

        picker.PickOptionAndDoAction(/* exitOnError */ false);
    }

    void SwitchAiProfile(TInteractiveConfigurationManager::TAiProfile::TPtr profile) {
        Y_DEBUG_VERIFY(profile, "Profile is not set");
        Y_DEBUG_VERIFY(AiModel && Controller, "AI session is not initialized");

        const auto& newProfileName = profile->GetName();
        if (newProfileName == AiModel->GetName()) {
            return;
        }

        if (HasContext) {
            Cout << Colors.Yellow() << "Active AI profile is changed to \"" << newProfileName << "\", session context will be reset" << Colors.OldColor();
        } else {
            Cout << "Switching AI profile to \"" << newProfileName << "\"";
        }

        ConfigurationManager->ChangeActiveAiProfile(newProfileName);
        AiModel = profile;
        Settings.Prompt = CreatePromptPrefix(ProfileName, AiModel->GetName());
        Controller->Setup(Settings);
        HasContext = false;
    }

private:
    const TString ProfileName;
    const TInteractiveConfigurationManager::TPtr ConfigurationManager;

    TInteractiveConfigurationManager::TAiProfile::TPtr AiModel;
    bool HasContext = false;
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings, const TInteractiveLogger& log) {
    return std::make_shared<NAi::TAiSessionRunner>(settings, log);
}

} // namespace NYdb::NConsoleClient
