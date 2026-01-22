#include "interactive_cli.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_config.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_settings.h>
#include <ydb/public/lib/ydb_cli/common/colors.h>
#include <ydb/public/lib/ydb_cli/common/log.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>
#if defined(YDB_CLI_AI_ENABLED)
#include <ydb/public/lib/ydb_cli/commands/interactive/session/ai_session_runner.h>
#endif
#include <ydb/public/lib/ydb_cli/commands/interactive/session/sql_session_runner.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sql.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <library/cpp/resource/resource.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>

namespace NYdb::NConsoleClient {

namespace {

constexpr char VersionResourceName[] = "version.txt";

} // anonymous namespace

TInteractiveCLI::TInteractiveCLI(const TString& profileName)
    : Profile(profileName)
{}

int TInteractiveCLI::Run(TClientCommand::TConfig& config) {
    const TDriver driver(config.CreateDriverConfig());
    const auto versionInfo = ResolveVersionInfo(driver);
    const auto configurationManager = std::make_shared<TInteractiveConfigurationManager>(config.AiProfileFile);

    if (config.EnableAiInteractive) {
        configurationManager->EnsurePredefinedProfiles(config.AiPredefinedProfiles, config.AiTokenGetter);
    }

    Cout << "Welcome to YDB CLI";
    if (versionInfo.CliVersion) {
        Cout << " " << TLogger::EntityName(versionInfo.CliVersion);
    }
    Cout << " " << configurationManager->ModeToString(configurationManager->GetDefaultMode()) << " interactive mode";

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

    if (config.EnableAiInteractive) {
        if (const auto& aiProfile = configurationManager->GetAiProfile(configurationManager->GetActiveAiProfileName())) {
            Cout << "AI profile: " << TLogger::EntityName(aiProfile->GetName()) << Endl;
        }
    }

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
    TString connectionString = connectionStringBuilder;

    ui64 activeSession = static_cast<ui64>(configurationManager->GetDefaultMode());
    if (!config.EnableAiInteractive) {
        activeSession = 0;
    }

    if (!activeSession && config.EnableAiInteractive) {
        Cout << "Type YQL query text or type " << TLogger::EntityNameQuoted("/help") << " for more info." << Endl << Endl;
    } else {
        Cout << "Type " << TLogger::EntityNameQuoted("/help") << " for more info." << Endl << Endl;
    }

    Y_VALIDATE(activeSession != static_cast<ui64>(TInteractiveConfigurationManager::EMode::Invalid), "Unexpected default mode: " << activeSession);

    std::vector<ISessionRunner::TPtr> sessions;

    sessions.push_back(CreateSqlSessionRunner({
        .Driver = driver,
        .Database = config.Database,
        .EnableAiInteractive = config.EnableAiInteractive,
    }));

#if defined(YDB_CLI_AI_ENABLED)
    if (config.EnableAiInteractive) {
        sessions.push_back(CreateAiSessionRunner({
            .ConfigurationManager = configurationManager,
            .Database = config.Database,
            .Driver = driver,
            .ConnectionString = connectionString,
        }));
    }
#else
    Y_UNUSED(connectionString);
#endif

    if (activeSession >= sessions.size()) {
        activeSession = 0;
    }

    ILineReader::TPtr lineReader;
    if (lineReader = sessions[activeSession]->Setup(); !lineReader) {
        YDB_CLI_LOG(Error, "Failed to perform initial setup in " << (activeSession ? "AI" : "SQL") << " mode");
        if (sessions.size() > 1) {
             Y_VALIDATE(lineReader = sessions[activeSession ^= 1]->Setup(), "Failed to change session to " << activeSession << " after error");
        } else {
             return EXIT_FAILURE;
        }
    }

    while (const auto inputOptional = lineReader->ReadLine()) {
        const auto& input = *inputOptional;
        if (std::holds_alternative<ILineReader::TSwitch>(input)) {
            if (sessions.size() > 1) {
                activeSession ^= 1;
                if (lineReader = sessions[activeSession]->Setup()) {
                    YDB_CLI_LOG(Info, "Switching to " << (activeSession ? "AI" : "SQL") << " mode");
                } else {
                    YDB_CLI_LOG(Error, "Failed to switch to " << (activeSession ? "AI" : "SQL") << " mode");
                    Y_VALIDATE(lineReader = sessions[activeSession ^= 1]->Setup(), "Failed to change session to " << activeSession << " after error");
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
    Cout << "Bye!" << Endl;

    return EXIT_SUCCESS;
}

TInteractiveCLI::TVersionInfo TInteractiveCLI::ResolveVersionInfo(const TDriver& driver) const {
    TVersionInfo result;

    try {
        result.CliVersion = StripString(NResource::Find(TStringBuf(VersionResourceName)));
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Error, "Couldn't read version from resource: " << e.what());
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
            result.ServerAvailableCheckFail = ToString(select1Status.GetStatus());
        }
    }

    return result;
}

} // namespace NYdb::NConsoleClient
