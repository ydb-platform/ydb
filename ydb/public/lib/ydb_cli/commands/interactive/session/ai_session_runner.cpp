#include "ai_session_runner.h"
#include "session_runner_common.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/ai/ai_model_handler.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/api_utils.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/common/ftxui.h>
#include <ydb/public/lib/ydb_cli/common/interactive.h>

#include <util/folder/dirut.h>
#include <util/system/env.h>

namespace NYdb::NConsoleClient {

namespace NAi {

namespace {

// TODO: move it into common paths file
constexpr int DIR_MODE_PRIVATE = S_IRUSR | S_IWUSR | S_IXUSR; // rwx------

void EnsureDir(const TFsPath& path, int mode) {
    if (path.Exists()) {
        return;
    }
#if defined(_win32_)
    Y_UNUSED(mode);
    path.MkDirs();
#else
    if (mode > 0) {
        path.MkDirs(mode);
    } else {
        path.MkDirs();
    }
#endif
}

TMaybe<TFsPath> GetEnvPath(const char* envName) {
    if (auto value = TryGetEnv(envName)) {
        if (!value->empty()) {
            return TFsPath(*value).Fix();
        }
    }
    return Nothing();
}

#if defined(_win32_)
TFsPath ResolveWindowsDir(const char* overrideEnv, const char* envName, std::initializer_list<TString> fallbackSuffixes) {
    if (auto overridePath = NAi::GetEnvPath(overrideEnv)) {
        return *overridePath;
    }
    if (auto envPath = NAi::GetEnvPath(envName)) {
        TFsPath path = *envPath;
        for (const auto& suffix : fallbackSuffixes) {
            path = path.Child(suffix);
        }
        return path.Fix();
    }
    TFsPath path = GetHomeDir();
    for (const auto& suffix : fallbackSuffixes) {
        path = path.Child(suffix);
    }
    return path.Fix();
}
#else
TFsPath ResolveUnixXdgDir(const char* overrideEnv, const char* xdgEnv, const TString& fallbackSuffix) {
    if (auto overridePath = NAi::GetEnvPath(overrideEnv)) {
        return *overridePath;
    }
    TString base;
    if (auto xdgPath = NAi::GetEnvPath(xdgEnv)) {
        base = xdgPath->GetPath();
    } else {
        base = TStringBuilder() << GetHomeDir() << fallbackSuffix;
    }
    return TFsPath(base).Child("ydb").Fix();
}
#endif

TFsPath GetStateDir() {
#if defined(_win32_)
    TFsPath dir = NAi::ResolveWindowsDir("YDB_STATE_DIR", "LOCALAPPDATA", {"ydb", "State"});
#else
    TFsPath dir = NAi::ResolveUnixXdgDir("YDB_STATE_DIR", "XDG_STATE_HOME", "/.local/state");
#endif
    NAi::EnsureDir(dir, NAi::DIR_MODE_PRIVATE);
    return dir;
}

TFsPath GetHistoryFile() {
    TFsPath stateDir = NAi::GetStateDir();
    TFsPath target = stateDir.Child("ai_history");
    return target;
}

class TAiSessionRunner final : public TSessionRunnerBase {
    using TBase = TSessionRunnerBase;

public:
    explicit TAiSessionRunner(const TAiSessionSettings& settings)
        : TBase(CreateSessionSettings(settings))
        , ConfigurationManager(settings.ConfigurationManager)
        , Database(settings.Database)
        , Driver(settings.Driver)
        , ConnectionString(settings.ConnectionString)
    {
        Y_VALIDATE(ConfigurationManager, "ConfigurationManager is not initialized");
    }

    ILineReader::TPtr Setup() final {
        ModelHandler.reset();

        AiModel = ConfigurationManager->ActivateAiProfile();
        if (!AiModel) {
            return nullptr;
        }

        TString validationError;
        Y_VALIDATE(AiModel->IsValid(validationError), "AI profile is not valid: " << validationError);
        return TBase::Setup();
    }

    void HandleLine(const TString& line) final {
        Y_VALIDATE(AiModel, "Can not handle input while AiModel is not initialized");
        Y_VALIDATE(ConfigurationManager->GetActiveAiProfileId() == AiModel->GetId(), "Unexpected active AI profile");

        if (!ModelHandler) {
            try {
                ModelHandler = TModelHandler({.Profile = AiModel, .Prompt = Settings.Prompt, .Database = Database, .Driver = Driver, .ConnectionString = ConnectionString});
            } catch (const std::exception& e) {
                ModelHandler = std::nullopt;
                Cerr << Colors.Red() << "Failed to setup AI model session: " << e.what() << Colors.OldColor() << Endl;
                return;
            }
        }

        if (to_lower(line) == "/help") {
            PrintFtxuiMessage(CreateHelpMessage(), "YDB CLI AI Interactive Mode – Hotkeys", ftxui::Color::White);
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

        std::shared_ptr<TProgressWaiterBase> spinner;
        double lastThinkingTime = 0.0;
        auto onStart = [&spinner]() {
            spinner = std::make_shared<TStaticProgressWaiter>("Agent is thinking...");
        };
        auto onFinish = [&spinner, &lastThinkingTime]() {
            if (spinner) {
                lastThinkingTime = spinner->Success().SecondsFloat();
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
            .HistoryFilePath = NAi::GetHistoryFile(),
            .AdditionalCommands = {"/help", "/model", "/config"},
            .Placeholder = "Type message (Enter to send, Ctrl+Enter for newline, Ctrl+T for YQL mode, Ctrl+D to exit)",
            .EnableYqlCompletion = false,
        };
    }

    void ChangeSessionSettings() {
        for (bool exit = false; !exit;) {
            std::vector<TMenuEntry> options;

            options.push_back({"Clear session context", [&]() {
                Cout << Endl << "Session context cleared." << Endl;
                if (ModelHandler) {
                    ModelHandler->ClearContext();
                }
                exit = true;
            }});

            Y_VALIDATE(AiModel, "Can not change session settings while AiModel is not initialized");
            options.emplace_back("Change current AI model settings", [&]() {
                if (!AiModel->Edit()) {
                    exit = true;
                    return;
                }

                ChangeAiProfile(AiModel);
            });

            options.emplace_back(TStringBuilder() << "Switch AI model\t" << AiModel->GetName(), [&]() {
                if (!SwitchAiProfile()) {
                    exit = true;
                }
            });

            options.emplace_back("Remove current AI model", [&]() {
                ConfigurationManager->RemoveAiProfile(AiModel->GetId());
                AiModel = ConfigurationManager->ActivateAiProfile();
                if (!AiModel) {
                    // Can not continue in AI mode
                    std::exit(EXIT_FAILURE);
                }

                ChangeAiProfile(AiModel);
            });

            if (!RunFtxuiMenuWithActions("Please choose setting to change:", options)) {
                exit = true;
            }
        }
    }

    bool SwitchAiProfile() {
        auto newProfile = ConfigurationManager->SelectAiProfile();

        if (newProfile) {
            ChangeAiProfile(std::move(newProfile));
            return true;
        }

        return false;
    }

    void ChangeAiProfile(TAiModelConfig::TPtr profile) {
        Y_VALIDATE(profile, "Profile is not set");
        Y_VALIDATE(AiModel, "AI session is not initialized");

        const auto& newProfileName = profile->GetName();
        if (ModelHandler) {
            Cout << Endl << Colors.Yellow() << "Active AI profile is changed"
                << (newProfileName != AiModel->GetName() ? TStringBuilder() << " to \"" << newProfileName << "\"" : TStringBuilder())
                << ", session context will be reset" << Colors.OldColor() << Endl;
        } else if (newProfileName != AiModel->GetName()) {
            Cout << Endl << "Switching AI profile to \"" << newProfileName << "\"" << Endl;
        }

        ConfigurationManager->Flush();
        AiModel = profile;
        ModelHandler.reset();
    }

private:
    const TInteractiveConfigurationManager::TPtr ConfigurationManager;
    const TString Database;
    const TDriver Driver;
    const TString ConnectionString;

    TAiModelConfig::TPtr AiModel;
    std::optional<TModelHandler> ModelHandler;
};

} // anonymous namespace

} // namespace NAi

ISessionRunner::TPtr CreateAiSessionRunner(const TAiSessionSettings& settings) {
    return std::make_shared<NAi::TAiSessionRunner>(settings);
}

} // namespace NYdb::NConsoleClient
