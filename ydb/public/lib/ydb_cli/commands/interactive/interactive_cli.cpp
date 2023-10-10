#include "interactive_cli.h"

#include <vector>

#include <util/folder/path.h>
#include <util/folder/dirut.h>

#include <ydb/public/lib/ydb_cli/common/query_stats.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_yql.h>

namespace NYdb {
namespace NConsoleClient {


namespace {

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
    const char * Position = nullptr;
};

Lexer::Lexer(std::string_view input)
    : Input(input)
    , Position(Input.data())
{}

std::optional<Token> Lexer::GetNextToken() {
    while (Position < Input.end() && std::isspace(*Position)) {
        ++Position;
    }

    if (Position == Input.end()) {
        return {};
    }

    const char * tokenStart = Position;
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

std::optional<NTable::ECollectQueryStatsMode> TryParseCollectStatsMode(const std::vector<Token> & tokens) {
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

void ParseSetCommand(const std::vector<Token> & tokens, InteractiveCLIState & interactiveCLIState) {
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

}

TInteractiveCLI::TInteractiveCLI(TClientCommand::TConfig & config, std::string prompt)
    : Config(config)
    , Prompt(std::move(prompt))
{}

void TInteractiveCLI::Run() {
    std::vector<std::string> SQLWords = {"SELECT", "FROM", "WHERE", "GROUP", "ORDER", "BY", "LIMIT", "OFFSET", 
        "EXPLAIN", "AST", "SET"};
    std::vector<std::string> Words;
    for (auto & word : SQLWords) {
        Words.push_back(word);
        Words.push_back(ToLower(word));
    }

    TFsPath homeDirPath(HomeDir);
    TString historyFilePath(homeDirPath / ".ydb_history");
    std::unique_ptr<ILineReader> lineReader = CreateLineReader(Prompt, historyFilePath, Suggest{std::move(Words)});

    InteractiveCLIState interactiveCLIState;

    while (auto lineOptional = lineReader->ReadLine())
    {
        auto & line = *lineOptional;
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
                explainCommand.Run(Config);
                continue;
            }

            TString queryStatsMode(NTable::QueryStatsModeToString(interactiveCLIState.CollectStatsMode));
            TCommandYql yqlCommand(TString(line), queryStatsMode);
            yqlCommand.Run(Config);
        } catch (TYdbErrorException &error) {
            Cerr << error;
        } catch (yexception & error) {
            Cerr << error;
        } catch (std::exception & error) {
            Cerr << error.what();
        }
    }

    std::cout << "Bye" << '\n';
}

}
}
