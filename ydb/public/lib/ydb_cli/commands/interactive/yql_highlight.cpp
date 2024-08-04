#include "yql_highlight.h"

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr/YQLLexer.h>

namespace NYdb {
    namespace NConsoleClient {

        YQLHighlight::ColorSchema YQLHighlight::ColorSchema::Default() {
            return {
                .keyword = Color::BLUE,
                .operation = Color::YELLOW,
                .identifier = Color::RED,
                .string = Color::GREEN,
                .number = Color::CYAN,
                .unknown = Color::DEFAULT,
            };
        }

        namespace {

            YQLHighlight::Color ColorOf(const YQLHighlight::ColorSchema& schema,
                                        const antlr4::Token* token) {
                switch (token->getType()) {
                    case antlr4::Token::INVALID_TYPE:
                        return schema.unknown;
                    case YQLLexer::STRING_VALUE:
                        return schema.string;
                    case YQLLexer::ID_PLAIN:
                    case YQLLexer::ID_QUOTED:
                        return schema.identifier;
                    case YQLLexer::INTEGER_VALUE:
                    case YQLLexer::REAL:
                    case YQLLexer::BLOB:
                    case YQLLexer::DIGITS:
                        return schema.number;
                    case YQLLexer::EQUALS:
                    case YQLLexer::EQUALS2:
                    case YQLLexer::NOT_EQUALS:
                    case YQLLexer::NOT_EQUALS2:
                    case YQLLexer::LESS:
                    case YQLLexer::LESS_OR_EQ:
                    case YQLLexer::GREATER:
                    case YQLLexer::GREATER_OR_EQ:
                    case YQLLexer::SHIFT_LEFT:
                    case YQLLexer::ROT_LEFT:
                    case YQLLexer::AMPERSAND:
                    case YQLLexer::PIPE:
                    case YQLLexer::DOUBLE_PIPE:
                    case YQLLexer::STRUCT_OPEN:
                    case YQLLexer::STRUCT_CLOSE:
                    case YQLLexer::PLUS:
                    case YQLLexer::MINUS:
                    case YQLLexer::TILDA:
                    case YQLLexer::SLASH:
                    case YQLLexer::ASTERISK:
                    case YQLLexer::BACKSLASH:
                    case YQLLexer::PERCENT:
                    case YQLLexer::SEMICOLON:
                    case YQLLexer::DOT:
                    case YQLLexer::COMMA:
                    case YQLLexer::LPAREN:
                    case YQLLexer::RPAREN:
                    case YQLLexer::QUESTION:
                    case YQLLexer::COLON:
                    case YQLLexer::AT:
                    case YQLLexer::DOUBLE_AT:
                    case YQLLexer::DOLLAR:
                    case YQLLexer::QUOTE_DOUBLE:
                    case YQLLexer::QUOTE_SINGLE:
                    case YQLLexer::BACKTICK:
                    case YQLLexer::LBRACE_CURLY:
                    case YQLLexer::RBRACE_CURLY:
                    case YQLLexer::CARET:
                    case YQLLexer::NAMESPACE:
                    case YQLLexer::ARROW:
                    case YQLLexer::RBRACE_SQUARE:
                    case YQLLexer::LBRACE_SQUARE:
                        return schema.operation;
                    default:
                        return schema.keyword;
                };
            }

        } // namespace

        YQLHighlight::YQLHighlight(ColorSchema color)
            : Coloring(color)
        {
        }

        void YQLHighlight::Apply(std::string_view query, Colors& colors) {
            antlr4::ANTLRInputStream chars(query);
            YQLLexer lexer(&chars);
            antlr4::BufferedTokenStream tokens(&lexer);

            lexer.removeErrorListeners();
            tokens.fill();

            for (std::size_t i = 0; i < tokens.size(); ++i) {
                const auto* token = tokens.get(i);
                const auto color = ColorOf(Coloring, token);

                const std::ptrdiff_t start = token->getStartIndex();
                const std::ptrdiff_t stop = token->getStopIndex() + 1;

                std::fill(std::next(std::begin(colors), start),
                          std::next(std::begin(colors), stop), color);
            }
        }

    } // namespace NConsoleClient
} // namespace NYdb
