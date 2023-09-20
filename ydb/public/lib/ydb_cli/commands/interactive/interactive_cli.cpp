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

template <typename Predicate>
void TrimRight(std::string & value, Predicate predicate) {
    while (!value.empty() && predicate(value.back()))
        value.pop_back();
}

template <typename Predicate>
void TrimLeft(std::string & value, Predicate predicate) {
    size_t value_size = value.size();
    size_t i = 0;

    for (; i < value_size; ++i) {
        if (!predicate(value[i])) {
            break;
        }
    }

    if (i != 0) {
        value = value.substr(i);
    }
}

void TrimSpacesRight(std::string & value) {
    TrimRight(value, [](char character) { return std::isspace(character); });
}

void TrimSpacesLeft(std::string & value) {
    TrimLeft(value, [](char character) { return std::isspace(character); });
}

struct Token {
    std::string_view data;
};

class Lexer {
public:
    Lexer(std::string_view input);

    std::optional<Token> GetNextToken();

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
    ++Position;

    while (Position < Input.end() && !std::isspace(*Position)) {
        ++Position;
    }

    std::string_view TokenData(tokenStart, Position);
    return Token{TokenData};
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
    if (tokensSize < 2) {
        return {};
    }

    if (ToLower(tokens[0].data) != "set") {
        return {};
    }

    std::string setQuery;
    for (size_t i = 1; i < tokensSize; ++i) {
        setQuery += tokens[i].data;
    }

    auto position = setQuery.find_first_of('=');
    if (position == std::string::npos) {
        return {};
    }

    std::string name = setQuery.substr(0, position);
    std::string value = setQuery.substr(position + 1);

    TrimSpacesLeft(name);
    TrimSpacesRight(name);
    if (name != "stats") {
        return {};
    }

    TrimSpacesLeft(value);
    TrimRight(value, [](char character) {
        return std::isspace(character) || character == ';';
    });

    return NTable::ParseQueryStatsMode(value);
}

}

TInteractiveCLI::TInteractiveCLI(TClientCommand::TConfig & config, std::string prompt)
    : Config(config)
    , Prompt(std::move(prompt))
{}

void TInteractiveCLI::Run() {
    std::vector<std::string> SQLWords = {"SELECT", "FROM", "WHERE", "GROUP", "ORDER" , "BY", "LIMIT", "OFFSET", "EXPLAIN", "AST"};
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

            if (auto collectStatsMode = TryParseCollectStatsMode(tokens)) {
                interactiveCLIState.CollectStatsMode = *collectStatsMode;
                continue;
            }

            if (!tokens.empty() && ToLower(tokens.front().data) == "explain") {
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
