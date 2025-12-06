#include "interactive_cli.h"

#include <ydb/core/base/validation.h>
#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sql.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>

#include <library/cpp/resource/resource.h>

#include <vector>

namespace NYdb::NConsoleClient {

namespace {

constexpr char VersionResourceName[] = "version.txt";

class TLexer {
public:
    struct TToken {
        std::string_view data;
    };

    explicit TLexer(std::string_view input)
        : Input(input)
        , Position(Input.data())
    {}

    static std::vector<TToken> Tokenize(std::string_view input) {
        std::vector<TToken> tokens;
        TLexer lexer(input);

        while (auto token = lexer.GetNextToken()) {
            tokens.push_back(*token);
        }

        return tokens;
    }

private:
    std::optional<TToken> GetNextToken() {
        while (Position < Input.end() && std::isspace(*Position)) {
            ++Position;
        }

        if (Position == Input.end()) {
            return {};
        }

        const char* tokenStart = Position;
        if (IsSeparatedTokenSymbol(*Position)) {
            ++Position;
        } else {
            while (Position < Input.end() && !std::isspace(*Position) && !IsSeparatedTokenSymbol(*Position)) {
                ++Position;
            }
        }

        std::string_view TokenData(tokenStart, Position);
        return TToken{TokenData};
    }

    static bool IsSeparatedTokenSymbol(char c) {
        return c == '=' || c == ';';
    }

private:
    std::string_view Input;
    const char* Position = nullptr;
};

class TInteractiveCLIState {
public:
    void TrySetCollectStatsMode(const std::vector<TLexer::TToken>& tokens) {
        size_t tokensSize = tokens.size();
        Y_DEBUG_VERIFY(tokensSize >= 4, "Not enough tokens for \"SET stats\" special command.");

        if (tokensSize > 4) {
            Cerr << "Variable value for \"SET stats\" special command should contain exactly one token." << Endl;
            return;
        }

        auto statsMode = NTable::ParseQueryStatsMode(tokens[3].data);
        if (!statsMode) {
            Cerr << "Unknown stats collection mode: \"" << tokens[3].data << "\"." << Endl;
        }

        CollectStatsMode = *statsMode;
    }

public:
    NTable::ECollectQueryStatsMode CollectStatsMode = NTable::ECollectQueryStatsMode::None;
};

void ParseSetCommand(const std::vector<TLexer::TToken>& tokens, TInteractiveCLIState& interactiveCLIState) {
    if (tokens.size() == 1) {
        Cerr << "Missing variable name for \"SET\" special command." << Endl;
    } else if (tokens.size() == 2 || tokens[2].data != "=") {
        Cerr << "Missing \"=\" symbol for \"SET\" special command." << Endl;
    } else if (tokens.size() == 3) {
        Cerr << "Missing variable value for \"SET\" special command." << Endl;
    } else if (to_lower(TString(tokens[1].data)) == "stats") {
        interactiveCLIState.TrySetCollectStatsMode(tokens);
    } else {
        Cerr << "Unknown variable name \"" << tokens[1].data << "\" for \"SET\" special command." << Endl;
    }
}

} // anonymous namespace

TInteractiveCLI::TInteractiveCLI(const TString& profileName, const TString& ydbPath)
    : Prompt(TStringBuilder() << Colors.LightGreen() << (profileName ? profileName : "ydb") << Colors.OldColor() << "> ")
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

    TString historyFilePath(TFsPath(YdbPath) / "test" / "interactive_cli_history.txt");
    const auto lineReader = CreateLineReader(Prompt, historyFilePath, driver, config.Database, Log);

    TInteractiveCLIState interactiveCLIState;
    while (const auto lineOptional = lineReader->ReadLine()) {
        auto& line = *lineOptional;
        if (line.empty()) {
            continue;
        }

        try {
            const auto& tokens = TLexer::Tokenize(line);
            if (tokens.empty()) {
                continue;
            }

            if (to_lower(TString(tokens[0].data)) == "set") {
                ParseSetCommand(tokens, interactiveCLIState);
                continue;
            }

            size_t tokensSize = tokens.size();
            if (to_lower(TString(tokens[0].data)) == "explain") {
                bool printAst = tokensSize >= 2 && to_lower(TString(tokens[1].data)) == "ast";
                size_t skipTokens = 1 + printAst;
                TString explainQuery;

                for (size_t i = skipTokens; i < tokensSize; ++i) {
                    explainQuery += tokens[i].data;
                    explainQuery += ' ';
                }

                TCommandExplain explainCommand(explainQuery, "data", printAst);
                explainCommand.Run(config);
                continue;
            }

            TString query = TString(line);
            TString queryLowerCase = to_lower(query);
            if (queryLowerCase == "quit" || queryLowerCase == "exit") {
                std::cout << "Bye" << std::endl;
                return EXIT_SUCCESS;
            }

            TString queryStatsMode(NTable::QueryStatsModeToString(interactiveCLIState.CollectStatsMode));
            TCommandSql sqlCommand;
            sqlCommand.SetScript(std::move(query));
            sqlCommand.SetCollectStatsMode(std::move(queryStatsMode));
            sqlCommand.SetSyntax("yql");
            sqlCommand.Run(config);
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
