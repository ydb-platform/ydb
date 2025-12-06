#include "interactive_cli.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/line_reader.h>
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
        Log.Error() << "Couldn't read version from resource: " << e.what();
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
        Log.Error() << "Couldn't read version from YDB server:" << Endl << TStatus(selectVersionResult);
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

    const auto sqlSession = CreateSqlSessionRunner({
        .ProfileName = Profile,
        .YdbPath = YdbPath,
        .Driver = driver,
    }, Log);

    const auto lineReader = CreateLineReader(driver, config.Database, Log);
    lineReader->Setup(sqlSession->GetSettings());

    while (const auto lineOptional = lineReader->ReadLine()) {
        const auto& line = *lineOptional;
        if (line.empty()) {
            continue;
        }

        try {
            sqlSession->HandleLine(line);
        } catch (NStatusHelpers::TYdbErrorException& error) {
            Cerr << colors.Red() << "Failed to handle command: " << colors.OldColor() << error;
        } catch (std::exception& error) {
            Cerr << colors.Red() << "Failed to handle command: " << colors.OldColor() << error.what();
        }
    }

    // Clear line (hints can be still present)
    lineReader->Finish();
    Cout << "Exiting." << Endl;

    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
