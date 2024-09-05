#include "yql_highlight.h"

#include <contrib/libs/antlr4_cpp_runtime/src/antlr4-runtime.h>
#include <regex>

#include <ydb/library/yql/parser/proto_ast/gen/v1_antlr4/SQLv1Antlr4Lexer.h>

#define TOKEN(NAME) SQLv1Antlr4Lexer::TOKEN_##NAME

namespace NYdb {
    namespace NConsoleClient {

        using NALPDefaultAntlr4::SQLv1Antlr4Lexer;

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
                .unknown = Color::DEFAULT,
            };
        }

        YQLHighlight::ColorSchema YQLHighlight::ColorSchema::Debug() {
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
                .unknown = Color::DEFAULT,
            };
        }

        YQLHighlight::YQLHighlight(ColorSchema color)
            : Coloring(color)
            , BuiltinFunctionRegex(builtinFunctionPattern, std::regex_constants::ECMAScript | std::regex_constants::icase)
            , TypeRegex(typePattern, std::regex_constants::ECMAScript | std::regex_constants::icase)
            , Chars()
            , Lexer(&Chars)
            , Tokens(&Lexer)
        {
            Lexer.removeErrorListeners();
        }

        void YQLHighlight::Apply(std::string_view query, Colors& colors) {
            Reset(query);

            for (std::size_t i = 0; i < Tokens.size(); ++i) {
                const auto* token = Tokens.get(i);
                const auto color = ColorOf(token);

                const std::ptrdiff_t start = token->getStartIndex();
                const std::ptrdiff_t stop = token->getStopIndex() + 1;

                std::fill(std::next(std::begin(colors), start),
                          std::next(std::begin(colors), stop), color);
            }
        }

        void YQLHighlight::Reset(std::string_view query) {
            Chars.load(query.data(), query.length());
            Lexer.reset();
            Tokens.setTokenSource(&Lexer);

            Tokens.fill();
        }

        YQLHighlight::Color YQLHighlight::ColorOf(const antlr4::Token* token) {
            if (IsString(token)) {
                return Coloring.string;
            }
            if (IsFunctionIdentifier(token)) {
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
            return Coloring.unknown;
        }

        bool YQLHighlight::IsKeyword(const antlr4::Token* token) const {
            switch (token->getType()) {
                case TOKEN(ABORT):
                case TOKEN(ACTION):
                case TOKEN(ADD):
                case TOKEN(AFTER):
                case TOKEN(ALL):
                case TOKEN(ALTER):
                case TOKEN(ANALYZE):
                case TOKEN(AND):
                case TOKEN(ANSI):
                case TOKEN(ANY):
                case TOKEN(ARRAY):
                case TOKEN(AS):
                case TOKEN(ASC):
                case TOKEN(ASSUME):
                case TOKEN(ASYMMETRIC):
                case TOKEN(ASYNC):
                case TOKEN(ATTACH):
                case TOKEN(ATTRIBUTES):
                case TOKEN(AUTOINCREMENT):
                case TOKEN(AUTOMAP):
                case TOKEN(BEFORE):
                case TOKEN(BEGIN):
                case TOKEN(BERNOULLI):
                case TOKEN(BETWEEN):
                case TOKEN(BITCAST):
                case TOKEN(BY):
                case TOKEN(CALLABLE):
                case TOKEN(CASCADE):
                case TOKEN(CASE):
                case TOKEN(CAST):
                case TOKEN(CHANGEFEED):
                case TOKEN(CHECK):
                case TOKEN(COLLATE):
                case TOKEN(COLUMN):
                case TOKEN(COLUMNS):
                case TOKEN(COMMIT):
                case TOKEN(COMPACT):
                case TOKEN(CONDITIONAL):
                case TOKEN(CONFLICT):
                case TOKEN(CONNECT):
                case TOKEN(CONSTRAINT):
                case TOKEN(CONSUMER):
                case TOKEN(COVER):
                case TOKEN(CREATE):
                case TOKEN(CROSS):
                case TOKEN(CUBE):
                case TOKEN(CURRENT):
                case TOKEN(CURRENT_DATE):
                case TOKEN(CURRENT_TIME):
                case TOKEN(CURRENT_TIMESTAMP):
                case TOKEN(DATA):
                case TOKEN(DATABASE):
                case TOKEN(DECIMAL):
                case TOKEN(DECLARE):
                case TOKEN(DEFAULT):
                case TOKEN(DEFERRABLE):
                case TOKEN(DEFERRED):
                case TOKEN(DEFINE):
                case TOKEN(DELETE):
                case TOKEN(DESC):
                case TOKEN(DESCRIBE):
                case TOKEN(DETACH):
                case TOKEN(DICT):
                case TOKEN(DIRECTORY):
                case TOKEN(DISABLE):
                case TOKEN(DISCARD):
                case TOKEN(DISTINCT):
                case TOKEN(DO):
                case TOKEN(DROP):
                case TOKEN(EACH):
                case TOKEN(ELSE):
                case TOKEN(EMPTY):
                case TOKEN(EMPTY_ACTION):
                case TOKEN(ENCRYPTED):
                case TOKEN(END):
                case TOKEN(ENUM):
                case TOKEN(ERASE):
                case TOKEN(ERROR):
                case TOKEN(ESCAPE):
                case TOKEN(EVALUATE):
                case TOKEN(EXCEPT):
                case TOKEN(EXCLUDE):
                case TOKEN(EXCLUSION):
                case TOKEN(EXCLUSIVE):
                case TOKEN(EXISTS):
                case TOKEN(EXPLAIN):
                case TOKEN(EXPORT):
                case TOKEN(EXTERNAL):
                case TOKEN(FAIL):
                case TOKEN(FALSE):
                case TOKEN(FAMILY):
                case TOKEN(FILTER):
                case TOKEN(FIRST):
                case TOKEN(FLATTEN):
                case TOKEN(FLOW):
                case TOKEN(FOLLOWING):
                case TOKEN(FOR):
                case TOKEN(FOREIGN):
                case TOKEN(FROM):
                case TOKEN(FULL):
                case TOKEN(FUNCTION):
                case TOKEN(GLOB):
                case TOKEN(GLOBAL):
                case TOKEN(GRANT):
                case TOKEN(GROUP):
                case TOKEN(GROUPING):
                case TOKEN(GROUPS):
                case TOKEN(HASH):
                case TOKEN(HAVING):
                case TOKEN(HOP):
                case TOKEN(IF):
                case TOKEN(IGNORE):
                case TOKEN(ILIKE):
                case TOKEN(IMMEDIATE):
                case TOKEN(IMPORT):
                case TOKEN(IN):
                case TOKEN(INDEX):
                case TOKEN(INDEXED):
                case TOKEN(INHERITS):
                case TOKEN(INITIAL):
                case TOKEN(INITIALLY):
                case TOKEN(INNER):
                case TOKEN(INSERT):
                case TOKEN(INSTEAD):
                case TOKEN(INTERSECT):
                case TOKEN(INTO):
                case TOKEN(IS):
                case TOKEN(ISNULL):
                case TOKEN(JOIN):
                case TOKEN(JSON_EXISTS):
                case TOKEN(JSON_QUERY):
                case TOKEN(JSON_VALUE):
                case TOKEN(KEY):
                case TOKEN(LAST):
                case TOKEN(LEFT):
                case TOKEN(LEGACY):
                case TOKEN(LIKE):
                case TOKEN(LIMIT):
                case TOKEN(LIST):
                case TOKEN(LOCAL):
                case TOKEN(MANAGE):
                case TOKEN(MATCH):
                case TOKEN(MATCHES):
                case TOKEN(MATCH_RECOGNIZE):
                case TOKEN(MEASURES):
                case TOKEN(MICROSECONDS):
                case TOKEN(MILLISECONDS):
                case TOKEN(MODIFY):
                case TOKEN(NANOSECONDS):
                case TOKEN(NATURAL):
                case TOKEN(NEXT):
                case TOKEN(NO):
                case TOKEN(NOT):
                case TOKEN(NOTNULL):
                case TOKEN(NULL):
                case TOKEN(NULLS):
                case TOKEN(OBJECT):
                case TOKEN(OF):
                case TOKEN(OFFSET):
                case TOKEN(OMIT):
                case TOKEN(ON):
                case TOKEN(ONE):
                case TOKEN(ONLY):
                case TOKEN(OPTION):
                case TOKEN(OPTIONAL):
                case TOKEN(OR):
                case TOKEN(ORDER):
                case TOKEN(OTHERS):
                case TOKEN(OUTER):
                case TOKEN(OVER):
                case TOKEN(PARALLEL):
                case TOKEN(PARTITION):
                case TOKEN(PASSING):
                case TOKEN(PASSWORD):
                case TOKEN(PAST):
                case TOKEN(PATTERN):
                case TOKEN(PER):
                case TOKEN(PERMUTE):
                case TOKEN(PLAN):
                case TOKEN(PRAGMA):
                case TOKEN(PRECEDING):
                case TOKEN(PRESORT):
                case TOKEN(PRIMARY):
                case TOKEN(PRIVILEGES):
                case TOKEN(PROCESS):
                case TOKEN(QUEUE):
                case TOKEN(RAISE):
                case TOKEN(RANGE):
                case TOKEN(REDUCE):
                case TOKEN(REFERENCES):
                case TOKEN(REGEXP):
                case TOKEN(REINDEX):
                case TOKEN(RELEASE):
                case TOKEN(REMOVE):
                case TOKEN(RENAME):
                case TOKEN(REPEATABLE):
                case TOKEN(REPLACE):
                case TOKEN(REPLICATION):
                case TOKEN(RESET):
                case TOKEN(RESOURCE):
                case TOKEN(RESPECT):
                case TOKEN(RESTRICT):
                case TOKEN(RESULT):
                case TOKEN(RETURN):
                case TOKEN(RETURNING):
                case TOKEN(REVERT):
                case TOKEN(REVOKE):
                case TOKEN(RIGHT):
                case TOKEN(RLIKE):
                case TOKEN(ROLLBACK):
                case TOKEN(ROLLUP):
                case TOKEN(ROW):
                case TOKEN(ROWS):
                case TOKEN(SAMPLE):
                case TOKEN(SAVEPOINT):
                case TOKEN(SCHEMA):
                case TOKEN(SECONDS):
                case TOKEN(SEEK):
                case TOKEN(SELECT):
                case TOKEN(SEMI):
                case TOKEN(SET):
                case TOKEN(SETS):
                case TOKEN(SHOW):
                case TOKEN(SOURCE):
                case TOKEN(STREAM):
                case TOKEN(STRUCT):
                case TOKEN(SUBQUERY):
                case TOKEN(SUBSET):
                case TOKEN(SYMBOLS):
                case TOKEN(SYMMETRIC):
                case TOKEN(SYNC):
                case TOKEN(SYSTEM):
                case TOKEN(TABLE):
                case TOKEN(TABLES):
                case TOKEN(TABLESAMPLE):
                case TOKEN(TABLESTORE):
                case TOKEN(TAGGED):
                case TOKEN(TEMP):
                case TOKEN(TEMPORARY):
                case TOKEN(THEN):
                case TOKEN(TIES):
                case TOKEN(TO):
                case TOKEN(TOPIC):
                case TOKEN(TRANSACTION):
                case TOKEN(TRIGGER):
                case TOKEN(TRUE):
                case TOKEN(TUPLE):
                case TOKEN(TYPE):
                case TOKEN(UNBOUNDED):
                case TOKEN(UNCONDITIONAL):
                case TOKEN(UNION):
                case TOKEN(UNIQUE):
                case TOKEN(UNKNOWN):
                case TOKEN(UNMATCHED):
                case TOKEN(UPDATE):
                case TOKEN(UPSERT):
                case TOKEN(USE):
                case TOKEN(USER):
                case TOKEN(USING):
                case TOKEN(VACUUM):
                case TOKEN(VALUES):
                case TOKEN(VARIANT):
                case TOKEN(VIEW):
                case TOKEN(VIRTUAL):
                case TOKEN(WHEN):
                case TOKEN(WHERE):
                case TOKEN(WINDOW):
                case TOKEN(WITH):
                case TOKEN(WITHOUT):
                case TOKEN(WRAPPER):
                case TOKEN(XOR):
                    return true;
                default:
                    return false;
            }
        }

        bool YQLHighlight::IsOperation(const antlr4::Token* token) const {
            switch (token->getType()) {
                case TOKEN(EQUALS):
                case TOKEN(EQUALS2):
                case TOKEN(NOT_EQUALS):
                case TOKEN(NOT_EQUALS2):
                case TOKEN(LESS):
                case TOKEN(LESS_OR_EQ):
                case TOKEN(GREATER):
                case TOKEN(GREATER_OR_EQ):
                case TOKEN(SHIFT_LEFT):
                case TOKEN(ROT_LEFT):
                case TOKEN(AMPERSAND):
                case TOKEN(PIPE):
                case TOKEN(DOUBLE_PIPE):
                case TOKEN(STRUCT_OPEN):
                case TOKEN(STRUCT_CLOSE):
                case TOKEN(PLUS):
                case TOKEN(MINUS):
                case TOKEN(TILDA):
                case TOKEN(ASTERISK):
                case TOKEN(SLASH):
                case TOKEN(BACKSLASH):
                case TOKEN(PERCENT):
                case TOKEN(SEMICOLON):
                case TOKEN(DOT):
                case TOKEN(COMMA):
                case TOKEN(LPAREN):
                case TOKEN(RPAREN):
                case TOKEN(QUESTION):
                case TOKEN(COLON):
                case TOKEN(AT):
                case TOKEN(DOUBLE_AT):
                case TOKEN(DOLLAR):
                case TOKEN(QUOTE_DOUBLE):
                case TOKEN(QUOTE_SINGLE):
                case TOKEN(BACKTICK):
                case TOKEN(LBRACE_CURLY):
                case TOKEN(RBRACE_CURLY):
                case TOKEN(CARET):
                case TOKEN(NAMESPACE):
                case TOKEN(ARROW):
                case TOKEN(RBRACE_SQUARE):
                case TOKEN(LBRACE_SQUARE):
                    return true;
                default:
                    return false;
            }
        }

        bool YQLHighlight::IsFunctionIdentifier(const antlr4::Token* token) {
            if (token->getType() != TOKEN(ID_PLAIN)) {
                return false;
            }
            const auto index = token->getTokenIndex();
            return std::regex_search(token->getText(), BuiltinFunctionRegex) ||
                   (2 <= index && Tokens.get(index - 1)->getType() == TOKEN(NAMESPACE) && Tokens.get(index - 2)->getType() == TOKEN(ID_PLAIN)) ||
                   (index < Tokens.size() - 1 && Tokens.get(index + 1)->getType() == TOKEN(NAMESPACE));
        }

        bool YQLHighlight::IsTypeIdentifier(const antlr4::Token* token) const {
            return token->getType() == TOKEN(ID_PLAIN) && std::regex_search(token->getText(), TypeRegex);
        }

        bool YQLHighlight::IsVariableIdentifier(const antlr4::Token* token) const {
            return token->getType() == TOKEN(ID_PLAIN);
        }

        bool YQLHighlight::IsQuotedIdentifier(const antlr4::Token* token) const {
            return token->getType() == TOKEN(ID_QUOTED);
        }

        bool YQLHighlight::IsString(const antlr4::Token* token) const {
            return token->getType() == TOKEN(STRING_VALUE);
        }

        bool YQLHighlight::IsNumber(const antlr4::Token* token) const {
            switch (token->getType()) {
                case TOKEN(DIGITS):
                case TOKEN(INTEGER_VALUE):
                case TOKEN(REAL):
                case TOKEN(BLOB):
                    return true;
                default:
                    return false;
            }
        }

    } // namespace NConsoleClient
} // namespace NYdb
