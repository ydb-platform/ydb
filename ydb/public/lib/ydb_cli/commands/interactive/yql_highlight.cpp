#include "yql_highlight.h"

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>
#include <ydb/public/lib/ydb_cli/commands/interactive/antlr/YQLLexer.h>

namespace NYdb {
    namespace NConsoleClient {

        YQLHighlight::ColorSchema YQLHighlight::ColorSchema::Default() {
            using replxx::color::rgb666;

            return {
                .keyword = Color::BLUE,
                .operation = rgb666(3, 3, 3),
                .simple_identifier = Color::DEFAULT,
                .quoted_identifier = rgb666(1, 3, 3),
                .string = rgb666(3, 0, 0),
                .number = Color::BRIGHTGREEN,
                .unknown = Color::DEFAULT,
            };
        }

        namespace {

            bool IsKeyword(const antlr4::Token* token) {
                switch (token->getType()) {
                    case YQLLexer::ABORT:
                    case YQLLexer::ACTION:
                    case YQLLexer::ADD:
                    case YQLLexer::AFTER:
                    case YQLLexer::ALL:
                    case YQLLexer::ALTER:
                    case YQLLexer::ANALYZE:
                    case YQLLexer::AND:
                    case YQLLexer::ANSI:
                    case YQLLexer::ANY:
                    case YQLLexer::ARRAY:
                    case YQLLexer::AS:
                    case YQLLexer::ASC:
                    case YQLLexer::ASSUME:
                    case YQLLexer::ASYMMETRIC:
                    case YQLLexer::ASYNC:
                    case YQLLexer::ATTACH:
                    case YQLLexer::ATTRIBUTES:
                    case YQLLexer::AUTOINCREMENT:
                    case YQLLexer::AUTOMAP:
                    case YQLLexer::BEFORE:
                    case YQLLexer::BEGIN:
                    case YQLLexer::BERNOULLI:
                    case YQLLexer::BETWEEN:
                    case YQLLexer::BITCAST:
                    case YQLLexer::BY:
                    case YQLLexer::CALLABLE:
                    case YQLLexer::CASCADE:
                    case YQLLexer::CASE:
                    case YQLLexer::CAST:
                    case YQLLexer::CHANGEFEED:
                    case YQLLexer::CHECK:
                    case YQLLexer::COLLATE:
                    case YQLLexer::COLUMN:
                    case YQLLexer::COLUMNS:
                    case YQLLexer::COMMIT:
                    case YQLLexer::COMPACT:
                    case YQLLexer::CONDITIONAL:
                    case YQLLexer::CONFLICT:
                    case YQLLexer::CONNECT:
                    case YQLLexer::CONSTRAINT:
                    case YQLLexer::CONSUMER:
                    case YQLLexer::COVER:
                    case YQLLexer::CREATE:
                    case YQLLexer::CROSS:
                    case YQLLexer::CUBE:
                    case YQLLexer::CURRENT:
                    case YQLLexer::CURRENT_DATE:
                    case YQLLexer::CURRENT_TIME:
                    case YQLLexer::CURRENT_TIMESTAMP:
                    case YQLLexer::DATA:
                    case YQLLexer::DATABASE:
                    case YQLLexer::DECIMAL:
                    case YQLLexer::DECLARE:
                    case YQLLexer::DEFAULT:
                    case YQLLexer::DEFERRABLE:
                    case YQLLexer::DEFERRED:
                    case YQLLexer::DEFINE:
                    case YQLLexer::DELETE:
                    case YQLLexer::DESC:
                    case YQLLexer::DESCRIBE:
                    case YQLLexer::DETACH:
                    case YQLLexer::DICT:
                    case YQLLexer::DIRECTORY:
                    case YQLLexer::DISABLE:
                    case YQLLexer::DISCARD:
                    case YQLLexer::DISTINCT:
                    case YQLLexer::DO:
                    case YQLLexer::DROP:
                    case YQLLexer::EACH:
                    case YQLLexer::ELSE:
                    case YQLLexer::EMPTY:
                    case YQLLexer::EMPTY_ACTION:
                    case YQLLexer::ENCRYPTED:
                    case YQLLexer::END:
                    case YQLLexer::ENUM:
                    case YQLLexer::ERASE:
                    case YQLLexer::ERROR:
                    case YQLLexer::ESCAPE:
                    case YQLLexer::EVALUATE:
                    case YQLLexer::EXCEPT:
                    case YQLLexer::EXCLUDE:
                    case YQLLexer::EXCLUSION:
                    case YQLLexer::EXCLUSIVE:
                    case YQLLexer::EXISTS:
                    case YQLLexer::EXPLAIN:
                    case YQLLexer::EXPORT:
                    case YQLLexer::EXTERNAL:
                    case YQLLexer::FAIL:
                    case YQLLexer::FALSE:
                    case YQLLexer::FAMILY:
                    case YQLLexer::FILTER:
                    case YQLLexer::FIRST:
                    case YQLLexer::FLATTEN:
                    case YQLLexer::FLOW:
                    case YQLLexer::FOLLOWING:
                    case YQLLexer::FOR:
                    case YQLLexer::FOREIGN:
                    case YQLLexer::FROM:
                    case YQLLexer::FULL:
                    case YQLLexer::FUNCTION:
                    case YQLLexer::GLOB:
                    case YQLLexer::GLOBAL:
                    case YQLLexer::GRANT:
                    case YQLLexer::GROUP:
                    case YQLLexer::GROUPING:
                    case YQLLexer::GROUPS:
                    case YQLLexer::HASH:
                    case YQLLexer::HAVING:
                    case YQLLexer::HOP:
                    case YQLLexer::IF:
                    case YQLLexer::IGNORE:
                    case YQLLexer::ILIKE:
                    case YQLLexer::IMMEDIATE:
                    case YQLLexer::IMPORT:
                    case YQLLexer::IN:
                    case YQLLexer::INDEX:
                    case YQLLexer::INDEXED:
                    case YQLLexer::INHERITS:
                    case YQLLexer::INITIAL:
                    case YQLLexer::INITIALLY:
                    case YQLLexer::INNER:
                    case YQLLexer::INSERT:
                    case YQLLexer::INSTEAD:
                    case YQLLexer::INTERSECT:
                    case YQLLexer::INTO:
                    case YQLLexer::IS:
                    case YQLLexer::ISNULL:
                    case YQLLexer::JOIN:
                    case YQLLexer::JSON_EXISTS:
                    case YQLLexer::JSON_QUERY:
                    case YQLLexer::JSON_VALUE:
                    case YQLLexer::KEY:
                    case YQLLexer::LAST:
                    case YQLLexer::LEFT:
                    case YQLLexer::LEGACY:
                    case YQLLexer::LIKE:
                    case YQLLexer::LIMIT:
                    case YQLLexer::LIST:
                    case YQLLexer::LOCAL:
                    case YQLLexer::MANAGE:
                    case YQLLexer::MATCH:
                    case YQLLexer::MATCHES:
                    case YQLLexer::MATCH_RECOGNIZE:
                    case YQLLexer::MEASURES:
                    case YQLLexer::MICROSECONDS:
                    case YQLLexer::MILLISECONDS:
                    case YQLLexer::MODIFY:
                    case YQLLexer::NANOSECONDS:
                    case YQLLexer::NATURAL:
                    case YQLLexer::NEXT:
                    case YQLLexer::NO:
                    case YQLLexer::NOT:
                    case YQLLexer::NOTNULL:
                    case YQLLexer::NULL_:
                    case YQLLexer::NULLS:
                    case YQLLexer::OBJECT:
                    case YQLLexer::OF:
                    case YQLLexer::OFFSET:
                    case YQLLexer::OMIT:
                    case YQLLexer::ON:
                    case YQLLexer::ONE:
                    case YQLLexer::ONLY:
                    case YQLLexer::OPTION:
                    case YQLLexer::OPTIONAL:
                    case YQLLexer::OR:
                    case YQLLexer::ORDER:
                    case YQLLexer::OTHERS:
                    case YQLLexer::OUTER:
                    case YQLLexer::OVER:
                    case YQLLexer::PARALLEL:
                    case YQLLexer::PARTITION:
                    case YQLLexer::PASSING:
                    case YQLLexer::PASSWORD:
                    case YQLLexer::PAST:
                    case YQLLexer::PATTERN:
                    case YQLLexer::PER:
                    case YQLLexer::PERMUTE:
                    case YQLLexer::PLAN:
                    case YQLLexer::PRAGMA:
                    case YQLLexer::PRECEDING:
                    case YQLLexer::PRESORT:
                    case YQLLexer::PRIMARY:
                    case YQLLexer::PRIVILEGES:
                    case YQLLexer::PROCESS:
                    case YQLLexer::QUEUE:
                    case YQLLexer::RAISE:
                    case YQLLexer::RANGE:
                    case YQLLexer::REDUCE:
                    case YQLLexer::REFERENCES:
                    case YQLLexer::REGEXP:
                    case YQLLexer::REINDEX:
                    case YQLLexer::RELEASE:
                    case YQLLexer::REMOVE:
                    case YQLLexer::RENAME:
                    case YQLLexer::REPEATABLE:
                    case YQLLexer::REPLACE:
                    case YQLLexer::REPLICATION:
                    case YQLLexer::RESET:
                    case YQLLexer::RESOURCE:
                    case YQLLexer::RESPECT:
                    case YQLLexer::RESTRICT:
                    case YQLLexer::RESULT:
                    case YQLLexer::RETURN:
                    case YQLLexer::RETURNING:
                    case YQLLexer::REVERT:
                    case YQLLexer::REVOKE:
                    case YQLLexer::RIGHT:
                    case YQLLexer::RLIKE:
                    case YQLLexer::ROLLBACK:
                    case YQLLexer::ROLLUP:
                    case YQLLexer::ROW:
                    case YQLLexer::ROWS:
                    case YQLLexer::SAMPLE:
                    case YQLLexer::SAVEPOINT:
                    case YQLLexer::SCHEMA:
                    case YQLLexer::SECONDS:
                    case YQLLexer::SEEK:
                    case YQLLexer::SELECT:
                    case YQLLexer::SEMI:
                    case YQLLexer::SET:
                    case YQLLexer::SETS:
                    case YQLLexer::SHOW:
                    case YQLLexer::SKIP_RULE:
                    case YQLLexer::SOURCE:
                    case YQLLexer::STREAM:
                    case YQLLexer::STRUCT:
                    case YQLLexer::SUBQUERY:
                    case YQLLexer::SUBSET:
                    case YQLLexer::SYMBOLS:
                    case YQLLexer::SYMMETRIC:
                    case YQLLexer::SYNC:
                    case YQLLexer::SYSTEM:
                    case YQLLexer::TABLE:
                    case YQLLexer::TABLES:
                    case YQLLexer::TABLESAMPLE:
                    case YQLLexer::TABLESTORE:
                    case YQLLexer::TAGGED:
                    case YQLLexer::TEMP:
                    case YQLLexer::TEMPORARY:
                    case YQLLexer::THEN:
                    case YQLLexer::TIES:
                    case YQLLexer::TO:
                    case YQLLexer::TOPIC:
                    case YQLLexer::TRANSACTION:
                    case YQLLexer::TRIGGER:
                    case YQLLexer::TRUE:
                    case YQLLexer::TUPLE:
                    case YQLLexer::TYPE:
                    case YQLLexer::UNBOUNDED:
                    case YQLLexer::UNCONDITIONAL:
                    case YQLLexer::UNION:
                    case YQLLexer::UNIQUE:
                    case YQLLexer::UNKNOWN:
                    case YQLLexer::UNMATCHED:
                    case YQLLexer::UPDATE:
                    case YQLLexer::UPSERT:
                    case YQLLexer::USE:
                    case YQLLexer::USER:
                    case YQLLexer::USING:
                    case YQLLexer::VACUUM:
                    case YQLLexer::VALUES:
                    case YQLLexer::VARIANT:
                    case YQLLexer::VIEW:
                    case YQLLexer::VIRTUAL:
                    case YQLLexer::WHEN:
                    case YQLLexer::WHERE:
                    case YQLLexer::WINDOW:
                    case YQLLexer::WITH:
                    case YQLLexer::WITHOUT:
                    case YQLLexer::WRAPPER:
                    case YQLLexer::XOR:
                        return true;
                    default:
                        return false;
                }
            }

            bool IsOperation(const antlr4::Token* token) {
                switch (token->getType()) {
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
                    case YQLLexer::ASTERISK:
                    case YQLLexer::SLASH:
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
                        return true;
                    default:
                        return false;
                }
            }

            bool IsSimpleIdentifier(const antlr4::Token* token) {
                switch (token->getType()) {
                    case YQLLexer::ID_PLAIN:
                        return true;
                    default:
                        return false;
                }
            }

            bool IsQuotedIdentifier(const antlr4::Token* token) {
                switch (token->getType()) {
                    case YQLLexer::ID_QUOTED:
                        return true;
                    default:
                        return false;
                }
            }

            bool IsString(const antlr4::Token* token) {
                switch (token->getType()) {
                    case YQLLexer::STRING_VALUE:
                        return true;
                    default:
                        return false;
                }
            }

            bool IsNumber(const antlr4::Token* token) {
                switch (token->getType()) {
                    case YQLLexer::DIGITS:
                    case YQLLexer::INTEGER_VALUE:
                    case YQLLexer::REAL:
                    case YQLLexer::BLOB:
                        return true;
                    default:
                        return false;
                }
            }

            YQLHighlight::Color ColorOf(const YQLHighlight::ColorSchema& schema,
                                        const antlr4::Token* token) {
                if (IsString(token)) {
                    return schema.string;
                }
                if (IsSimpleIdentifier(token)) {
                    return schema.simple_identifier;
                }
                if (IsQuotedIdentifier(token)) {
                    return schema.quoted_identifier;
                }
                if (IsNumber(token)) {
                    return schema.number;
                }
                if (IsOperation(token)) {
                    return schema.operation;
                }
                if (IsKeyword(token)) {
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
