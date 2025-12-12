#include "interactive_cli.h"

#include <ydb/library/yverify_stream/yverify_stream.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/interactive_log_defs.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/common/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/session/ai_session_runner.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/session/sql_session_runner.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sql.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>

#include <library/cpp/resource/resource.h>

namespace NYdb::NConsoleClient {

namespace {

constexpr char VersionResourceName[] = "version.txt";

} // anonymous namespace

TInteractiveCLI::TInteractiveCLI(const TString& profileName, const TString& ydbPath)
    : Profile(profileName)
    , YdbPath(ydbPath)
{}

int TInteractiveCLI::Run(TClientCommand::TConfig& config) {
    Log.Setup(config);

    TString cliVersion;
    try {
        cliVersion = StripString(NResource::Find(TStringBuf(VersionResourceName)));
    } catch (const std::exception& e) {
        YDB_CLI_LOG(Error, "Couldn't read version from resource: " << e.what());
        cliVersion.clear();
    }

    const auto& colors = NColorizer::AutoColors(Cout);
    Cout << "Welcome to YDB CLI";
    if (!cliVersion.empty()) {
        Cout << " " << colors.BoldColor() << cliVersion << colors.OldColor();
    }
    Cout << " interactive mode";

    TDriver driver(config.CreateDriverConfig());
    NQuery::TQueryClient client(driver);

    const auto selectVersionResult = client.RetryQuery([](NQuery::TSession session) {
        return session.ExecuteQuery("SELECT version()", NQuery::TTxControl::NoTx());
    }).ExtractValueSync();
    TString serverVersion;
    if (selectVersionResult.IsSuccess()) {
        try {
            auto parser = selectVersionResult.GetResultSetParser(0);
            parser.TryNextRow();
            serverVersion = parser.ColumnParser(0).GetString();
        } catch (const std::exception& e) {
            Cerr << Endl << colors.Red() << "Couldn't parse server version: " << colors.OldColor() << e.what() << Endl;
        }
    } else {
        YDB_CLI_LOG(Error, "Couldn't read version from YDB server:" << Endl << TStatus(selectVersionResult));
    }

    if (!serverVersion.empty()) {
        Cout << " (YDB Server " << colors.BoldColor() << serverVersion << colors.OldColor() << ")" << Endl;
    } else {
        Cout << Endl;
        const auto select1Status = client.RetryQuery([](NQuery::TSession session) {
            return session.ExecuteQuery("SELECT 1", NQuery::TTxControl::NoTx());
        }).ExtractValueSync();
        if (!select1Status.IsSuccess()) {
            Cerr << colors.Red() << "Couldn't connect to YDB server:" << colors.OldColor() << Endl << TStatus(select1Status) << Endl;
            return EXIT_FAILURE;
        }
    }

    Cout << "Press " << colors.BoldColor() << "Ctrl+K" << colors.OldColor() << " for more information." << Endl;

    const auto configurationManager = std::make_shared<TInteractiveConfigurationManager>(config.AiProfileFile, Log);
    ui64 activeSession = static_cast<ui64>(configurationManager->GetDefaultMode());
    Y_VALIDATE(activeSession != static_cast<ui64>(TInteractiveConfigurationManager::EMode::Invalid), "Unexpected default mode: " << activeSession);

    const std::vector sessions = {
        CreateSqlSessionRunner({
            .ProfileName = Profile,
            .YdbPath = YdbPath,
            .Driver = driver,
        }, Log),
        CreateAiSessionRunner({
            .ProfileName = Profile,
            .YdbPath = YdbPath,
            .ConfigurationManager = configurationManager,
            .Database = config.Database,
            .Driver = driver,
        }, Log),
    };
    Y_VALIDATE(sessions.size() > activeSession, "Unexpected number of sessions: " << sessions.size() << " for default mode: " << activeSession);

    const auto lineReader = CreateLineReader({.Driver = driver, .Database = config.Database}, Log);
    if (!sessions[activeSession]->Setup(lineReader)) {
        YDB_CLI_LOG(Error, "Failed to perform initial setup in " << (activeSession ? "AI" : "SQL") << " mode");
        Y_VALIDATE(sessions[activeSession ^= 1]->Setup(lineReader), "Failed to change session to " << activeSession << " after error");
    }

    while (const auto inputOptional = lineReader->ReadLine()) {
        const auto& input = *inputOptional;
        if (std::holds_alternative<ILineReader::TSwitch>(input)) {
            activeSession ^= 1;
            if (sessions[activeSession]->Setup(lineReader)) {
                YDB_CLI_LOG(Info, "Switching to " << (activeSession ? "AI" : "SQL") << " mode");
            } else {
                YDB_CLI_LOG(Error, "Failed to switch to " << (activeSession ? "AI" : "SQL") << " mode");
                Y_VALIDATE(sessions[activeSession ^= 1]->Setup(lineReader), "Failed to change session to " << activeSession << " after error");
            }
            continue;
        }

        const auto& line = Strip(std::get<ILineReader::TLine>(input).Data);
        if (line.empty()) {
            continue;
        }

        if (const auto input = to_lower(line); input == "quit" || input == "exit") {
            break;
        }

        try {
            sessions[activeSession]->HandleLine(line);
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << colors.Red() << "Failed to handle command:" << colors.OldColor() << Endl << error << Endl;
        } catch (std::exception& error) {
            Cerr << colors.Red() << "Failed to handle command:" << colors.OldColor() << Endl << error.what() << Endl;
        }
    }

    // Clear line (hints can be still present)
    lineReader->Finish();
    Cout << "Bye!" << Endl;

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
