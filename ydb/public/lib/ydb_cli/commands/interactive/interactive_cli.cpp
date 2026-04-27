#include "interactive_cli.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_settings.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/session/ai_session_runner.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/session/sql_session_runner.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sql.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/generic/scope.h>
#include <util/string/strip.h>

#include <csignal>

#if defined(_unix_)
#include <termios.h>
#include <unistd.h>
#endif

namespace NYdb::NConsoleClient {

namespace {

constexpr char VersionResourceName[] = "version.txt";

struct TVersionInfo {
    TString CliVersion;
    TString ServerVersion;
    TString ServerAvailableCheckFail;
};

TVersionInfo ResolveVersionInfo(const TDriver& driver) {
    TVersionInfo result;

    try {
        result.CliVersion = StripString(NResource::Find(TStringBuf(VersionResourceName)));
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Debug, "Couldn't read version from resource: " << e.what());
        result.CliVersion.clear();
    }

    NQuery::TQueryClient client(driver);
    const auto selectVersionResult = client.RetryQuery([](NQuery::TSession session) {
        return session.ExecuteQuery("SELECT version()", NQuery::TTxControl::NoTx());
    }).ExtractValueSync();

    if (selectVersionResult.IsSuccess()) {
        try {
            auto parser = selectVersionResult.GetResultSetParser(0);
            parser.TryNextRow();
            result.ServerVersion = parser.ColumnParser(0).GetString();
        } catch (const std::exception& e) {
            YDB_CLI_LOG(Error, "Couldn't read version from YDB server:\n" << TStatus(selectVersionResult));
        }
    } else {
        YDB_CLI_LOG(Error, "Couldn't read version from YDB server:\n" << TStatus(selectVersionResult));
    }

    if (!result.ServerVersion) {
        const auto select1Status = client.RetryQuery([](NQuery::TSession session) {
            return session.ExecuteQuery("SELECT 1", NQuery::TTxControl::NoTx());
        }).ExtractValueSync();

        if (!select1Status.IsSuccess()) {
            result.ServerAvailableCheckFail = TStringBuilder() << "Status: " << select1Status.GetStatus() << "Issues:\n" << select1Status.GetIssues().ToString();
        }
    }

    return result;
}

std::vector<ISessionRunner::TPtr> SetupSessions(const TClientCommand::TConfig& config, const TDriver& driver, TInteractiveConfigurationManager::TPtr configManager) {
    std::vector<ISessionRunner::TPtr> sessions;

    sessions.push_back(CreateSqlSessionRunner({
        .Driver = driver,
        .Database = config.Database,
        .EnableAiInteractive = config.EnableAiInteractive,
    }));

    if (config.EnableAiInteractive) {
        TStringBuilder connectionStringBuilder;
        // config.InitialArgC and config.InitialArgV contain all arguments passed to the CLI
        for (int i = 0; i < config.InitialArgC; ++i) {
            if (i > 0) {
                connectionStringBuilder << " ";
            }
            TString arg = config.InitialArgV[i];
            if (arg.Contains(' ')) {
                 connectionStringBuilder << "\"" << arg << "\"";
            } else {
                 connectionStringBuilder << arg;
            }
        }

        sessions.push_back(CreateAiSessionRunner({
            .ConfigurationManager = configManager,
            .Database = config.Database,
            .Driver = driver,
            .ConnectionString = connectionStringBuilder,
            .UsageInfoGetter = config.UsageInfoGetter,
        }));
    }

    return sessions;
}

} // anonymous namespace

TInteractiveCLI::TInteractiveCLI(const TString& profileName)
    : Profile(profileName)
{}

int TInteractiveCLI::Run(TClientCommand::TConfig& config) {
    config.BuildInfoCommandTag = "interactive";

    // Ctrl+C handling stays where it should: inside replxx. While a line is being read the
    // terminal is in raw mode with ISIG disabled, so the tty driver does not turn Ctrl+C
    // into SIGINT — it arrives as a \x03 byte that replxx interprets as a cancel event
    // (or as an exit when two cancels happen within DoubleCtrlCWindow).
    //
    // The reason we mask SIGINT here is purely to close a narrow race that appears every
    // time replxx switches the terminal back to canonical mode (on every ReadLine return
    // and at shutdown). Any \x03 bytes still queued in the pty buffer are then translated
    // by the tty driver into a real SIGINT, and the default disposition would kill the
    // process before "Bye!" is printed — even though replxx has already consumed the
    // logical Ctrl+C event. SIG_IGN simply discards these stragglers; user-visible Ctrl+C
    // behaviour is unchanged.
    const auto prevSigintHandler = std::signal(SIGINT, SIG_IGN);
    Y_DEFER {
        std::signal(SIGINT, prevSigintHandler);
    };

    const TDriver driver(config.CreateDriverConfig());
    const auto configManager = std::make_shared<TInteractiveConfigurationManager>(config.AiProfileFile, !config.EnableAiInteractive);
    if (auto code = PrintWelcomeMessage(config, driver, configManager)) {
        return code;
    }

    ui64 activeSession = static_cast<ui64>(configManager->GetInteractiveMode());
    const auto& sessions = SetupSessions(config, driver, configManager);
    Y_VALIDATE(activeSession < sessions.size(), "Invalid active session: " << activeSession);

    ILineReader::TPtr lineReader;
    if (lineReader = sessions[activeSession]->Setup(); !lineReader) {
        YDB_CLI_LOG(Notice, "Failed to perform initial setup in " << (activeSession ? "AI" : "SQL") << " mode");
        if (sessions.size() > 1) {
             Y_VALIDATE(lineReader = sessions[activeSession ^= 1]->Setup(), "Failed to change session to " << activeSession << " after error");
             configManager->SetInteractiveMode(static_cast<TInteractiveConfigurationManager::EMode>(activeSession));
        } else {
             return EXIT_FAILURE;
        }
    }

    Cout << Endl;
    while (const auto inputOptional = lineReader->ReadLine()) {
        Y_DEFER { Cout << Endl; };

        const auto& input = *inputOptional;
        if (std::holds_alternative<ILineReader::TSwitch>(input)) {
            if (sessions.size() > 1) {
                activeSession ^= 1;
                if (lineReader = sessions[activeSession]->Setup()) {
                    YDB_CLI_LOG(Info, "Switching to " << (activeSession ? "AI" : "SQL") << " mode");
                    configManager->SetInteractiveMode(static_cast<TInteractiveConfigurationManager::EMode>(activeSession));
                } else {
                    YDB_CLI_LOG(Info, "Failed to switch to " << (activeSession ? "AI" : "SQL") << " mode");
                    Y_VALIDATE(lineReader = sessions[activeSession ^= 1]->Setup(), "Failed to change session to " << activeSession << " after error");
                    configManager->SetInteractiveMode(static_cast<TInteractiveConfigurationManager::EMode>(activeSession));
                }
            }
            continue;
        }

        const auto& line = std::get<ILineReader::TLine>(input).Data;
        if (line.empty()) {
            continue;
        }

        if (const auto input = to_lower(line); input == "quit" || input == "exit") {
            break;
        }

        try {
            sessions[activeSession]->HandleLine(line);
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << Colors.Red() << "Failed to handle command:" << Colors.OldColor() << Endl << error << Endl;
        } catch (std::exception& error) {
            Cerr << Colors.Red() << "Failed to handle command:" << Colors.OldColor() << Endl << error.what() << Endl;
        }
    }

    // Clear line (hints can be still present)
    lineReader->Finish(true);

    // Drop any Ctrl+C bytes the user may have sent but that replxx didn't consume, so the tty
    // driver doesn't echo stale "^C" sequences after the farewell message.
#if defined(_unix_)
    tcflush(STDIN_FILENO, TCIFLUSH);
#endif

    Cout << "Bye!" << Endl;

    return EXIT_SUCCESS;
}

int TInteractiveCLI::PrintWelcomeMessage(const TClientCommand::TConfig& config, const TDriver& driver, TInteractiveConfigurationManager::TPtr configManager) const {
    const auto versionInfo = ResolveVersionInfo(driver);

    if (!config.EnableAiInteractive) {
        configManager->SetInteractiveMode(TInteractiveConfigurationManager::EMode::YQL);
    }

    Cout << "Welcome to YDB CLI";
    if (versionInfo.CliVersion) {
        Cout << " " << TLogger::EntityName(versionInfo.CliVersion);
    }
    Cout << " " << configManager->ModeToString(configManager->GetInteractiveMode()) << " interactive mode";

    if (versionInfo.ServerVersion) {
        Cout << " (YDB Server " << TLogger::EntityName(versionInfo.ServerVersion) << ")" << Endl;
    } else if (versionInfo.ServerAvailableCheckFail) {
        Cout << Endl;
        Cerr << Colors.Red() << "Couldn't connect to YDB server:\n" << versionInfo.ServerAvailableCheckFail << Colors.OldColor() << Endl;
        return EXIT_FAILURE;
    }

    if (Profile) {
        Cout << "Connection profile: " << TLogger::EntityName(Profile) << Endl;
    } else {
        Cout << "Endpoint: " << TLogger::EntityName(config.Address) << Endl;
        Cout << "Database: " << TLogger::EntityName(config.Database) << Endl;
    }

    if (config.EnableAiInteractive && configManager->GetInteractiveMode() == TInteractiveConfigurationManager::EMode::AI) {
        TString activeProfileName;
        if (const auto& activeProfileId = configManager->GetActiveAiProfileId()) {
            if (const auto aiProfile = configManager->ActivateAiProfile(activeProfileId)) {
                activeProfileName = aiProfile->GetName();
            }
        }

        if (activeProfileName) {
            Cout << "Using model: " << TLogger::EntityName(activeProfileName) << Endl;            
        } else if (!configManager->ActivateAiProfile("", /* printWelcomeMessage */ false)) {
            configManager->SetInteractiveMode(TInteractiveConfigurationManager::EMode::YQL);
            Cout << Endl << "Switching to " << configManager->ModeToString(configManager->GetInteractiveMode()) << " interactive mode, use " << TLogger::EntityNameQuoted("/switch") << " to change mode." << Endl;
        }
    }

    if (configManager->GetInteractiveMode() == TInteractiveConfigurationManager::EMode::YQL) {
        Cout << "Type YQL query text or type " << TLogger::EntityNameQuoted("/help") << " for more info." << Endl;
    } else {
        Cout << "Type " << TLogger::EntityNameQuoted("/help") << " for more info or " << TLogger::EntityNameQuoted("/model") << " to select another model." << Endl;
    }

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
