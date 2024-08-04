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
                const auto type = token->getType();
                if (type == YQLLexer::STRING_VALUE) {
                    return schema.string;
                }
                if (YQLLexer::ID_PLAIN <= type && type <= YQLLexer::ID_QUOTED) {
                    return schema.identifier;
                }
                if (YQLLexer::DIGITS <= type && type <= YQLLexer::BLOB) {
                    return schema.number;
                }
                if (YQLLexer::EQUALS <= type && type <= YQLLexer::LBRACE_SQUARE) {
                    return schema.operation;
                }
                if (YQLLexer::ABORT <= type && type <= YQLLexer::XOR) {
                    return schema.keyword;
                }
                return schema.unknown;
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
