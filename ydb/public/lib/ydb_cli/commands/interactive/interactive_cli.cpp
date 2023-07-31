#include "interactive_cli.h"

#include <vector>

#include <util/folder/path.h>
#include <util/folder/dirut.h>

#include <ydb/public/lib/ydb_cli/commands/interactive/line_reader.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_scheme.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_service_table.h>
#include <ydb/public/lib/ydb_cli/commands/ydb_yql.h>

namespace NYdb {
namespace NConsoleClient {


namespace {

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
    std::string CollectStatsMode = "none";
};

}

TInteractiveCLI::TInteractiveCLI(TClientCommand::TConfig & config, std::string prompt)
    : Config(config)
    , Prompt(std::move(prompt))
{}

void TInteractiveCLI::Run() {
    std::vector<std::string> SQLWords = {"SELECT", "FROM", "WHERE", "GROUP", "ORDER" , "BY", "LIMIT", "OFFSET"};
    std::vector<std::string> Words;
    for (auto & word : SQLWords) {
        Words.push_back(word);
        for (auto & character : word) {
            character = std::tolower(character);
        }

        Words.push_back(word);
    }

    TFsPath homeDirPath(GetHomeDir());
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

            if (tokensSize == 4 && tokens[0].data == "SET" && tokens[1].data == "stats" && tokens[2].data == "=") {
                interactiveCLIState.CollectStatsMode = tokens[3].data;
                while (!interactiveCLIState.CollectStatsMode.empty() && interactiveCLIState.CollectStatsMode.back() == ';')
                    interactiveCLIState.CollectStatsMode.pop_back();
                continue;
            }

            if (!tokens.empty() && tokens.front().data == "EXPLAIN") {
                bool printAst = tokensSize >= 2 && tokens[1].data == "AST";
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

            TCommandYql yqlCommand(TString(line), TString(interactiveCLIState.CollectStatsMode));
            yqlCommand.Run(Config);
        } catch (TYdbErrorException &error) {
            Cerr << error;
        }
    }

    std::cout << "Bye" << '\n';
}

}
}
