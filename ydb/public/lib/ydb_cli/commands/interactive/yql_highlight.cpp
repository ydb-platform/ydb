#include "yql_highlight.h"

#include <ydb/library/yql/parser/lexer_common/lexer.h>
#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>
#include <ydb/library/yql/sql/v1/lexer/lexer.h>

#include <regex>

#define TOKEN(NAME) SQLv1Antlr4Lexer::TOKEN_##NAME

namespace NYdb {
    namespace NConsoleClient {
        using NSQLTranslationV1::MakeLexer;
        using NSQLTranslation::SQL_MAX_PARSER_ERRORS;
        using NYql::TIssues;

        constexpr const char* builtinFunctionPattern = ( //
            "^("
            "abs|aggregate_by|aggregate_list|aggregate_list_distinct|agg_list|agg_list_distinct|"
            "as_table|avg|avg_if|adaptivedistancehistogram|adaptivewardhistogram|adaptiveweighthistogram|"
            "addmember|addtimezone|aggregateflatten|aggregatetransforminput|aggregatetransformoutput|"
            "aggregationfactory|asatom|asdict|asdictstrict|asenum|aslist|asliststrict|asset|assetstrict|"
            "asstruct|astagged|astuple|asvariant|atomcode|bitcast|bit_and|bit_or|bit_xor|bool_and|"
            "bool_or|bool_xor|bottom|bottom_by|blockwardhistogram|blockweighthistogram|cast|coalesce|"
            "concat|concat_strict|correlation|count|count_if|covariance|covariance_population|"
            "covariance_sample|callableargument|callableargumenttype|callableresulttype|callabletype|"
            "callabletypecomponents|callabletypehandle|choosemembers|combinemembers|countdistinctestimate|"
            "currentauthenticateduser|currentoperationid|currentoperationsharedid|currenttzdate|"
            "currenttzdatetime|currenttztimestamp|currentutcdate|currentutcdatetime|currentutctimestamp|"
            "dense_rank|datatype|datatypecomponents|datatypehandle|dictaggregate|dictcontains|dictcreate|"
            "dicthasitems|dictitems|dictkeytype|dictkeys|dictlength|dictlookup|dictpayloadtype|dictpayloads|"
            "dicttype|dicttypecomponents|dicttypehandle|each|each_strict|emptydicttype|emptydicttypehandle|"
            "emptylisttype|emptylisttypehandle|endswith|ensure|ensureconvertibleto|ensuretype|enum|"
            "evaluateatom|evaluatecode|evaluateexpr|evaluatetype|expandstruct|filter|filter_strict|find|"
            "first_value|folder|filecontent|filepath|flattenmembers|forceremovemember|forceremovemembers|"
            "forcerenamemembers|forcespreadmembers|formatcode|formattype|frombytes|frompg|funccode|"
            "greatest|grouping|gathermembers|generictype|histogram|hll|hoppingwindowpgcast|hyperloglog|"
            "if|if_strict|instanceof|json_exists|json_query|json_value|jointablerow|just|lag|last_value|"
            "lead|least|len|length|like|likely|like_strict|lambdaargumentscount|lambdacode|"
            "lambdaoptionalargumentscount|linearhistogram|listaggregate|listall|listany|listavg|listcode|"
            "listcollect|listconcat|listcreate|listdistinct|listenumerate|listextend|listextendstrict|"
            "listextract|listfilter|listflatmap|listflatten|listfold|listfold1|listfold1map|listfoldmap|"
            "listfromrange|listfromtuple|listhas|listhasitems|listhead|listindexof|listitemtype|listlast|"
            "listlength|listmap|listmax|listmin|listnotnull|listreplicate|listreverse|listskip|"
            "listskipwhile|listskipwhileinclusive|listsort|listsortasc|listsortdesc|listsum|listtake|"
            "listtakewhile|listtakewhileinclusive|listtotuple|listtype|listtypehandle|listunionall|"
            "listuniq|listzip|listzipall|loghistogram|logarithmichistogram|max|max_by|max_of|median|"
            "min|min_by|min_of|mode|multi_aggregate_by|nanvl|nvl|nothing|nulltype|nulltypehandle|"
            "optionalitemtype|optionaltype|optionaltypehandle|percentile|parsefile|parsetype|"
            "parsetypehandle|pgand|pgarray|pgcall|pgconst|pgnot|pgop|pgor|pickle|quotecode|range|"
            "range_strict|rank|regexp|regexp_strict|rfind|row_number|random|randomnumber|randomuuid|"
            "removemember|removemembers|removetimezone|renamemembers|replacemember|reprcode|resourcetype|"
            "resourcetypehandle|resourcetypetag|some|stddev|stddev_population|stddev_sample|substring|"
            "sum|sum_if|sessionstart|sessionwindow|setcreate|setdifference|setincludes|setintersection|"
            "setisdisjoint|setsymmetricdifference|setunion|spreadmembers|stablepickle|startswith|staticmap|"
            "staticzip|streamitemtype|streamtype|streamtypehandle|structmembertype|structmembers|"
            "structtypecomponents|structtypehandle|subqueryextend|subqueryextendfor|subquerymerge|"
            "subquerymergefor|subqueryunionall|subqueryunionallfor|subqueryunionmerge|subqueryunionmergefor"
            "|top|topfreq|top_by|tablename|tablepath|tablerecordindex|tablerow|tablerows|taggedtype|"
            "taggedtypecomponents|taggedtypehandle|tobytes|todict|tomultidict|topg|toset|tosorteddict|"
            "tosortedmultidict|trymember|tupleelementtype|tupletype|tupletypecomponents|tupletypehandle|"
            "typehandle|typekind|typeof|udaf|udf|unittype|unpickle|untag|unwrap|variance|variance_population|"
            "variance_sample|variant|varianttype|varianttypehandle|variantunderlyingtype|voidtype|"
            "voidtypehandle|way|worldcode|weakfield"
            ")$");

        constexpr const char* typePattern = ( //
            "^("
            "bool|date|datetime|decimal|double|float|int16|int32|int64|int8|interval|json|jsondocument|"
            "string|timestamp|tzdate|tzdatetime|tztimestamp|uint16|uint32|uint64|uint8|utf8|uuid|yson|"
            "text|bytes"
            ")$");

        YQLHighlight::ColorSchema YQLHighlight::ColorSchema::Monaco() {
            using replxx::color::rgb666;

            return {
                .keyword = Color::BLUE,
                .operation = rgb666(3, 3, 3),
                .identifier = {
                    .function = rgb666(4, 1, 5),
                    .type = rgb666(2, 3, 2),
                    .variable = Color::DEFAULT,
                    .quoted = rgb666(1, 3, 3),
                },
                .string = rgb666(3, 0, 0),
                .number = Color::BRIGHTGREEN,
                .comment = rgb666(2, 2, 2),
                .unknown = Color::DEFAULT,
            };
        }

        YQLHighlight::ColorSchema YQLHighlight::ColorSchema::Debug() {
            using replxx::color::rgb666;

            return {
                .keyword = Color::BLUE,
                .operation = Color::GRAY,
                .identifier = {
                    .function = Color::MAGENTA,
                    .type = Color::YELLOW,
                    .variable = Color::RED,
                    .quoted = Color::CYAN,
                },
                .string = Color::GREEN,
                .number = Color::BRIGHTGREEN,
                .comment = rgb666(2, 2, 2),
                .unknown = Color::DEFAULT,
            };
        }

        YQLHighlight::YQLHighlight(ColorSchema color)
            : Coloring(color)
            , BuiltinFunctionRegex(builtinFunctionPattern, std::regex_constants::ECMAScript | std::regex_constants::icase)
            , TypeRegex(typePattern, std::regex_constants::ECMAScript | std::regex_constants::icase)
            , Lexer(MakeLexer(/* ansi = */ false, /* antlr4 = */ true))
        {
        }

        void YQLHighlight::Apply(std::string_view query, Colors& colors) {
            Tokens = Tokenize(TString(query));
            for (std::size_t i = 0; i < Tokens.size() - 1; ++i) {
                const auto& token = Tokens.at(i);
                const auto color = ColorOf(token, i);

                const std::ptrdiff_t start = token.StartIndex;
                const std::ptrdiff_t stop = token.StartIndex + token.Content.Size();

                std::fill(std::next(std::begin(colors), start),
                          std::next(std::begin(colors), stop), color);
            }
        }

        TParsedTokenList YQLHighlight::Tokenize(const TString& query) {
            TParsedTokenList tokens;
            TIssues issues;
            NSQLTranslation::Tokenize(*Lexer, query, "Query", tokens, issues, SQL_MAX_PARSER_ERRORS);
            return tokens;
        }

        YQLHighlight::Color YQLHighlight::ColorOf(const TParsedToken& token, size_t index) {
            if (IsString(token)) {
                return Coloring.string;
            }
            if (IsFunctionIdentifier(token, index)) {
                return Coloring.identifier.function;
            }
            if (IsTypeIdentifier(token)) {
                return Coloring.identifier.type;
            }
            if (IsVariableIdentifier(token)) {
                return Coloring.identifier.variable;
            }
            if (IsQuotedIdentifier(token)) {
                return Coloring.identifier.quoted;
            }
            if (IsNumber(token)) {
                return Coloring.number;
            }
            if (IsOperation(token)) {
                return Coloring.operation;
            }
            if (IsKeyword(token)) {
                return Coloring.keyword;
            }
            if (IsComment(token)) {
                return Coloring.comment;
            }
            return Coloring.unknown;
        }

        bool YQLHighlight::IsKeyword(const TParsedToken& token) const {
            return (
                token.Name == "ABORT" ||
                token.Name == "ACTION" ||
                token.Name == "ADD" ||
                token.Name == "AFTER" ||
                token.Name == "ALL" ||
                token.Name == "ALTER" ||
                token.Name == "ANALYZE" ||
                token.Name == "AND" ||
                token.Name == "ANSI" ||
                token.Name == "ANY" ||
                token.Name == "ARRAY" ||
                token.Name == "AS" ||
                token.Name == "ASC" ||
                token.Name == "ASSUME" ||
                token.Name == "ASYMMETRIC" ||
                token.Name == "ASYNC" ||
                token.Name == "ATTACH" ||
                token.Name == "ATTRIBUTES" ||
                token.Name == "AUTOINCREMENT" ||
                token.Name == "AUTOMAP" ||
                token.Name == "BEFORE" ||
                token.Name == "BEGIN" ||
                token.Name == "BERNOULLI" ||
                token.Name == "BETWEEN" ||
                token.Name == "BITCAST" ||
                token.Name == "BY" ||
                token.Name == "CALLABLE" ||
                token.Name == "CASCADE" ||
                token.Name == "CASE" ||
                token.Name == "CAST" ||
                token.Name == "CHANGEFEED" ||
                token.Name == "CHECK" ||
                token.Name == "COLLATE" ||
                token.Name == "COLUMN" ||
                token.Name == "COLUMNS" ||
                token.Name == "COMMIT" ||
                token.Name == "COMPACT" ||
                token.Name == "CONDITIONAL" ||
                token.Name == "CONFLICT" ||
                token.Name == "CONNECT" ||
                token.Name == "CONSTRAINT" ||
                token.Name == "CONSUMER" ||
                token.Name == "COVER" ||
                token.Name == "CREATE" ||
                token.Name == "CROSS" ||
                token.Name == "CUBE" ||
                token.Name == "CURRENT" ||
                token.Name == "CURRENT_DATE" ||
                token.Name == "CURRENT_TIME" ||
                token.Name == "CURRENT_TIMESTAMP" ||
                token.Name == "DATA" ||
                token.Name == "DATABASE" ||
                token.Name == "DECIMAL" ||
                token.Name == "DECLARE" ||
                token.Name == "DEFAULT" ||
                token.Name == "DEFERRABLE" ||
                token.Name == "DEFERRED" ||
                token.Name == "DEFINE" ||
                token.Name == "DELETE" ||
                token.Name == "DESC" ||
                token.Name == "DESCRIBE" ||
                token.Name == "DETACH" ||
                token.Name == "DICT" ||
                token.Name == "DIRECTORY" ||
                token.Name == "DISABLE" ||
                token.Name == "DISCARD" ||
                token.Name == "DISTINCT" ||
                token.Name == "DO" ||
                token.Name == "DROP" ||
                token.Name == "EACH" ||
                token.Name == "ELSE" ||
                token.Name == "EMPTY" ||
                token.Name == "EMPTY_ACTION" ||
                token.Name == "ENCRYPTED" ||
                token.Name == "END" ||
                token.Name == "ENUM" ||
                token.Name == "ERASE" ||
                token.Name == "ERROR" ||
                token.Name == "ESCAPE" ||
                token.Name == "EVALUATE" ||
                token.Name == "EXCEPT" ||
                token.Name == "EXCLUDE" ||
                token.Name == "EXCLUSION" ||
                token.Name == "EXCLUSIVE" ||
                token.Name == "EXISTS" ||
                token.Name == "EXPLAIN" ||
                token.Name == "EXPORT" ||
                token.Name == "EXTERNAL" ||
                token.Name == "FAIL" ||
                token.Name == "FALSE" ||
                token.Name == "FAMILY" ||
                token.Name == "FILTER" ||
                token.Name == "FIRST" ||
                token.Name == "FLATTEN" ||
                token.Name == "FLOW" ||
                token.Name == "FOLLOWING" ||
                token.Name == "FOR" ||
                token.Name == "FOREIGN" ||
                token.Name == "FROM" ||
                token.Name == "FULL" ||
                token.Name == "FUNCTION" ||
                token.Name == "GLOB" ||
                token.Name == "GLOBAL" ||
                token.Name == "GRANT" ||
                token.Name == "GROUP" ||
                token.Name == "GROUPING" ||
                token.Name == "GROUPS" ||
                token.Name == "HASH" ||
                token.Name == "HAVING" ||
                token.Name == "HOP" ||
                token.Name == "IF" ||
                token.Name == "IGNORE" ||
                token.Name == "ILIKE" ||
                token.Name == "IMMEDIATE" ||
                token.Name == "IMPORT" ||
                token.Name == "IN" ||
                token.Name == "INDEX" ||
                token.Name == "INDEXED" ||
                token.Name == "INHERITS" ||
                token.Name == "INITIAL" ||
                token.Name == "INITIALLY" ||
                token.Name == "INNER" ||
                token.Name == "INSERT" ||
                token.Name == "INSTEAD" ||
                token.Name == "INTERSECT" ||
                token.Name == "INTO" ||
                token.Name == "IS" ||
                token.Name == "ISNULL" ||
                token.Name == "JOIN" ||
                token.Name == "JSON_EXISTS" ||
                token.Name == "JSON_QUERY" ||
                token.Name == "JSON_VALUE" ||
                token.Name == "KEY" ||
                token.Name == "LAST" ||
                token.Name == "LEFT" ||
                token.Name == "LEGACY" ||
                token.Name == "LIKE" ||
                token.Name == "LIMIT" ||
                token.Name == "LIST" ||
                token.Name == "LOCAL" ||
                token.Name == "MANAGE" ||
                token.Name == "MATCH" ||
                token.Name == "MATCHES" ||
                token.Name == "MATCH_RECOGNIZE" ||
                token.Name == "MEASURES" ||
                token.Name == "MICROSECONDS" ||
                token.Name == "MILLISECONDS" ||
                token.Name == "MODIFY" ||
                token.Name == "NANOSECONDS" ||
                token.Name == "NATURAL" ||
                token.Name == "NEXT" ||
                token.Name == "NO" ||
                token.Name == "NOT" ||
                token.Name == "NOTNULL" ||
                token.Name == "NULL" ||
                token.Name == "NULLS" ||
                token.Name == "OBJECT" ||
                token.Name == "OF" ||
                token.Name == "OFFSET" ||
                token.Name == "OMIT" ||
                token.Name == "ON" ||
                token.Name == "ONE" ||
                token.Name == "ONLY" ||
                token.Name == "OPTION" ||
                token.Name == "OPTIONAL" ||
                token.Name == "OR" ||
                token.Name == "ORDER" ||
                token.Name == "OTHERS" ||
                token.Name == "OUTER" ||
                token.Name == "OVER" ||
                token.Name == "PARALLEL" ||
                token.Name == "PARTITION" ||
                token.Name == "PASSING" ||
                token.Name == "PASSWORD" ||
                token.Name == "PAST" ||
                token.Name == "PATTERN" ||
                token.Name == "PER" ||
                token.Name == "PERMUTE" ||
                token.Name == "PLAN" ||
                token.Name == "PRAGMA" ||
                token.Name == "PRECEDING" ||
                token.Name == "PRESORT" ||
                token.Name == "PRIMARY" ||
                token.Name == "PRIVILEGES" ||
                token.Name == "PROCESS" ||
                token.Name == "QUEUE" ||
                token.Name == "RAISE" ||
                token.Name == "RANGE" ||
                token.Name == "REDUCE" ||
                token.Name == "REFERENCES" ||
                token.Name == "REGEXP" ||
                token.Name == "REINDEX" ||
                token.Name == "RELEASE" ||
                token.Name == "REMOVE" ||
                token.Name == "RENAME" ||
                token.Name == "REPEATABLE" ||
                token.Name == "REPLACE" ||
                token.Name == "REPLICATION" ||
                token.Name == "RESET" ||
                token.Name == "RESOURCE" ||
                token.Name == "RESPECT" ||
                token.Name == "RESTRICT" ||
                token.Name == "RESULT" ||
                token.Name == "RETURN" ||
                token.Name == "RETURNING" ||
                token.Name == "REVERT" ||
                token.Name == "REVOKE" ||
                token.Name == "RIGHT" ||
                token.Name == "RLIKE" ||
                token.Name == "ROLLBACK" ||
                token.Name == "ROLLUP" ||
                token.Name == "ROW" ||
                token.Name == "ROWS" ||
                token.Name == "SAMPLE" ||
                token.Name == "SAVEPOINT" ||
                token.Name == "SCHEMA" ||
                token.Name == "SECONDS" ||
                token.Name == "SEEK" ||
                token.Name == "SELECT" ||
                token.Name == "SEMI" ||
                token.Name == "SET" ||
                token.Name == "SETS" ||
                token.Name == "SHOW" ||
                token.Name == "SOURCE" ||
                token.Name == "STREAM" ||
                token.Name == "STRUCT" ||
                token.Name == "SUBQUERY" ||
                token.Name == "SUBSET" ||
                token.Name == "SYMBOLS" ||
                token.Name == "SYMMETRIC" ||
                token.Name == "SYNC" ||
                token.Name == "SYSTEM" ||
                token.Name == "TABLE" ||
                token.Name == "TABLES" ||
                token.Name == "TABLESAMPLE" ||
                token.Name == "TABLESTORE" ||
                token.Name == "TAGGED" ||
                token.Name == "TEMP" ||
                token.Name == "TEMPORARY" ||
                token.Name == "THEN" ||
                token.Name == "TIES" ||
                token.Name == "TO" ||
                token.Name == "TOPIC" ||
                token.Name == "TRANSACTION" ||
                token.Name == "TRIGGER" ||
                token.Name == "TRUE" ||
                token.Name == "TUPLE" ||
                token.Name == "TYPE" ||
                token.Name == "UNBOUNDED" ||
                token.Name == "UNCONDITIONAL" ||
                token.Name == "UNION" ||
                token.Name == "UNIQUE" ||
                token.Name == "UNKNOWN" ||
                token.Name == "UNMATCHED" ||
                token.Name == "UPDATE" ||
                token.Name == "UPSERT" ||
                token.Name == "USE" ||
                token.Name == "USER" ||
                token.Name == "USING" ||
                token.Name == "VACUUM" ||
                token.Name == "VALUES" ||
                token.Name == "VARIANT" ||
                token.Name == "VIEW" ||
                token.Name == "VIRTUAL" ||
                token.Name == "WHEN" ||
                token.Name == "WHERE" ||
                token.Name == "WINDOW" ||
                token.Name == "WITH" ||
                token.Name == "WITHOUT" ||
                token.Name == "WRAPPER" ||
                token.Name == "XOR"
            );
        }

        bool YQLHighlight::IsOperation(const TParsedToken& token) const {
            return (
                token.Name == "EQUALS" ||
                token.Name == "EQUALS2" ||
                token.Name == "NOT_EQUALS" ||
                token.Name == "NOT_EQUALS2" ||
                token.Name == "LESS" ||
                token.Name == "LESS_OR_EQ" ||
                token.Name == "GREATER" ||
                token.Name == "GREATER_OR_EQ" ||
                token.Name == "SHIFT_LEFT" ||
                token.Name == "ROT_LEFT" ||
                token.Name == "AMPERSAND" ||
                token.Name == "PIPE" ||
                token.Name == "DOUBLE_PIPE" ||
                token.Name == "STRUCT_OPEN" ||
                token.Name == "STRUCT_CLOSE" ||
                token.Name == "PLUS" ||
                token.Name == "MINUS" ||
                token.Name == "TILDA" ||
                token.Name == "ASTERISK" ||
                token.Name == "SLASH" ||
                token.Name == "BACKSLASH" ||
                token.Name == "PERCENT" ||
                token.Name == "SEMICOLON" ||
                token.Name == "DOT" ||
                token.Name == "COMMA" ||
                token.Name == "LPAREN" ||
                token.Name == "RPAREN" ||
                token.Name == "QUESTION" ||
                token.Name == "COLON" ||
                token.Name == "AT" ||
                token.Name == "DOUBLE_AT" ||
                token.Name == "DOLLAR" ||
                token.Name == "QUOTE_DOUBLE" ||
                token.Name == "QUOTE_SINGLE" ||
                token.Name == "BACKTICK" ||
                token.Name == "LBRACE_CURLY" ||
                token.Name == "RBRACE_CURLY" ||
                token.Name == "CARET" ||
                token.Name == "NAMESPACE" ||
                token.Name == "ARROW" ||
                token.Name == "RBRACE_SQUARE" ||
                token.Name == "LBRACE_SQUARE"
            );
        }

        bool YQLHighlight::IsFunctionIdentifier(const TParsedToken& token, size_t index) {
            if (token.Name != "ID_PLAIN") {
                return false;
            }
            return std::regex_search(token.Content.begin(), token.Content.end(), BuiltinFunctionRegex) ||
                   (2 <= index && Tokens.at(index - 1).Name == "NAMESPACE" && Tokens.at(index - 2).Name == "ID_PLAIN") ||
                   (index < Tokens.size() - 1 && Tokens.at(index + 1).Name == "NAMESPACE");
        }

        bool YQLHighlight::IsTypeIdentifier(const TParsedToken& token) const {
            return token.Name == "ID_PLAIN" && std::regex_search(token.Content.begin(), token.Content.end(), TypeRegex);
        }

        bool YQLHighlight::IsVariableIdentifier(const TParsedToken& token) const {
            return token.Name == "ID_PLAIN";
        }

        bool YQLHighlight::IsQuotedIdentifier(const TParsedToken& token) const {
            return token.Name == "ID_QUOTED";
        }

        bool YQLHighlight::IsString(const TParsedToken& token) const {
            return token.Name == "STRING_VALUE";
        }

        bool YQLHighlight::IsNumber(const TParsedToken& token) const {
            return (
                token.Name == "DIGITS" ||
                token.Name == "INTEGER_VALUE" ||
                token.Name == "REAL" ||
                token.Name == "BLOB"
            );
        }

        bool YQLHighlight::IsComment(const TParsedToken& token) const {
            return token.Name == "COMMENT";
        }

    } // namespace NConsoleClient
} // namespace NYdb
