#include "interactive_cli.h"

#include <vector>

#include <library/cpp/resource/resource.h>
#include <util/folder/path.h>
#include <util/folder/dirut.h>
#include <util/string/strip.h>

#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_sql.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/query/client.h>

namespace NYdb::NConsoleClient {

namespace {

const char* VersionResourceName = "version.txt";

std::string ToLower(std::string_view value) {
    size_t value_size = value.size();
    std::string result;
    result.resize(value_size);

    for (size_t i = 0; i < value_size; ++i) {
        result[i] = std::tolower(value[i]);
    }

    return result;
}

struct Token {
    std::string_view data;
};

class Lexer {
public:
    Lexer(std::string_view input);

    std::optional<Token> GetNextToken();

    static bool IsSeparatedTokenSymbol(char c);

private:
    std::string_view Input;
    const char* Position = nullptr;
};

Lexer::Lexer(std::string_view input)
    : Input(input)
    , Position(Input.data())
{
}

std::optional<Token> Lexer::GetNextToken() {
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
    return Token{TokenData};
}

bool Lexer::IsSeparatedTokenSymbol(char c) {
    return c == '=' || c == ';';
}

std::vector<Token> Tokenize(std::string_view input) {
    std::vector<Token> tokens;
    Lexer lexer(input);

    while (auto token = lexer.GetNextToken()) {
        tokens.push_back(*token);
    }

    return tokens;
}

struct InteractiveCLIState {
    NTable::ECollectQueryStatsMode CollectStatsMode = NTable::ECollectQueryStatsMode::None;
};

std::optional<NTable::ECollectQueryStatsMode> TryParseCollectStatsMode(const std::vector<Token>& tokens) {
    size_t tokensSize = tokens.size();

    if (tokensSize > 4) {
        Cerr << "Variable value for \"SET stats\" special command should contain exactly one token." << Endl;
        return {};
    }

    auto statsMode = NTable::ParseQueryStatsMode(tokens[3].data);
    if (!statsMode) {
        Cerr << "Unknown stats collection mode: \"" << tokens[3].data << "\"." << Endl;
    }
    return statsMode;
}

void ParseSetCommand(const std::vector<Token>& tokens, InteractiveCLIState& interactiveCLIState) {
    if (tokens.size() == 1) {
        Cerr << "Missing variable name for \"SET\" special command." << Endl;
    } else if (tokens.size() == 2 || tokens[2].data != "=") {
        Cerr << "Missing \"=\" symbol for \"SET\" special command." << Endl;
    } else if (tokens.size() == 3) {
        Cerr << "Missing variable value for \"SET\" special command." << Endl;
    } else if (ToLower(tokens[1].data) == "stats") {
        if (auto statsMode = TryParseCollectStatsMode(tokens)) {
            interactiveCLIState.CollectStatsMode = *statsMode;
        }
    } else {
        Cerr << "Unknown variable name \"" << tokens[1].data << "\" for \"SET\" special command." << Endl;
    }
}

} // namespace

TInteractiveCLI::TInteractiveCLI(std::string prompt)
    : Prompt(std::move(prompt))
{
}

int TInteractiveCLI::Run(TClientCommand::TConfig& config) {
    std::string cliVersion;
    try {
        cliVersion = StripString(NResource::Find(TStringBuf(VersionResourceName)));
    } catch (const std::exception& e) {
        cliVersion.clear();
    }

    NColorizer::TColors colors = NColorizer::AutoColors(Cout);
    Cout << "Welcome to YDB CLI";
    if (!cliVersion.empty()) {
        Cout << " " << colors.BoldColor() << cliVersion << colors.OldColor() ;
    }
    Cout << " interactive mode";

    auto driver = TDriver(config.CreateDriverConfig());
    NQuery::TQueryClient client(driver);
    auto selectVersionResult = client.RetryQuery([](NQuery::TSession session) {
        return session.ExecuteQuery("SELECT version() AS version;", NQuery::TTxControl::NoTx());
    }).GetValueSync();
    std::string serverVersion;
    if (selectVersionResult.IsSuccess()) {
        try {
            auto parser = selectVersionResult.GetResultSetParser(0);
            parser.TryNextRow();
            serverVersion = parser.ColumnParser("version").GetString();
        } catch (const std::exception& e) {
            Cerr << Endl << "Couldn't parse server version: " << e.what() << Endl;
        }
    }
    if (!serverVersion.empty()) {
        Cout << " (YDB Server " << colors.BoldColor() << serverVersion << colors.OldColor() << ")" << Endl;
    } else {
        Cout << Endl;
        auto select1Status = client.RetryQuerySync([](NQuery::TSession session) {
            return session.ExecuteQuery("SELECT 1;", NQuery::TTxControl::NoTx()).GetValueSync();
        });
        if (!select1Status.IsSuccess()) {
            Cerr << "Couldn't connect to YDB server:" << Endl << select1Status << Endl;
            return EXIT_FAILURE;
        }
    }

    Cout << colors.BoldColor() << "Hotkeys:" << colors.OldColor() << Endl
        << "  " << colors.BoldColor() << "Up and Down arrow keys" << colors.OldColor() << ": navigate through query history." << Endl
        << "  " << colors.BoldColor() << "TAB" << colors.OldColor() << ": complete the current word based on YQL syntax." << Endl
        << "  " << colors.BoldColor() << "Ctrl+R" << colors.OldColor() << ": search for a query in history containing a specified substring." << Endl
        << "  " << colors.BoldColor() << "Ctrl+D" << colors.OldColor() << ": exit interactive mode." << Endl
        << Endl;

    TFsPath homeDirPath(HomeDir);
    TString historyFilePath(homeDirPath / ".ydb_history");
    std::unique_ptr<ILineReader> lineReader = CreateLineReader(Prompt, historyFilePath, config);

    InteractiveCLIState interactiveCLIState;

    while (auto lineOptional = lineReader->ReadLine())
    {
        auto& line = *lineOptional;
        if (line.empty()) {
            continue;
        }

        try {
            auto tokens = Tokenize(line);
            size_t tokensSize = tokens.size();
            if (tokens.empty()) {
                continue;
            }

            if (ToLower(tokens[0].data) == "set") {
                ParseSetCommand(tokens, interactiveCLIState);
                continue;
            }

            if (ToLower(tokens[0].data) == "explain") {
                bool printAst = tokensSize >= 2 && ToLower(tokens[1].data) == "ast";
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
            Cerr << error;
        } catch (yexception& error) {
            Cerr << error;
        } catch (std::exception& error) {
            Cerr << error.what();
        }
    }

    // Clear line (hints can be still present) and print "Bye"
    std::cout << std::endl << "\r\033[2KBye" << std::endl;
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient
