#include "sql_ut.h"

#include <yql/essentials/providers/common/provider/yql_provider_names.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <google/protobuf/arena.h>

#include <array>

using namespace NSQLTranslationV1;

Y_UNIT_TEST_SUITE(ColumnDefaultExpr) {

namespace {

struct TDefaultSourceTestCase {
    TStringBuf Name;
    TStringBuf Query;
    TStringBuf ExpectedExpression;
};

void AssertDefaultSource(const TDefaultSourceTestCase& testCase) {
    const auto res = SqlToYql(TString(testCase.Query));
    UNIT_ASSERT_C(res.IsOk(), testCase.Name << ": " << Err2Str(res));

    const auto program = GetPrettyPrint(res);

    TString expectedSource = "'\"";
    expectedSource += testCase.ExpectedExpression;
    expectedSource += "\"";

    UNIT_ASSERT_C(program.Contains(expectedSource), testCase.Name << ": expected DEFAULT source " << expectedSource << " in:\n"
                                                                  << program);
}

struct TTranslationEnvironment {
    TTranslationEnvironment() {
        Settings.Arena = &Arena;
        Settings.ClusterMapping["ydb"] = NYql::YdbProviderName;
        Settings.Mode = NSQLTranslation::ESqlMode::QUERY;
        Settings.SyntaxVersion = 1;

        Lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
        Parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory();
    }

    google::protobuf::Arena Arena;
    NSQLTranslation::TTranslationSettings Settings;
    NSQLTranslationV1::TLexers Lexers;
    NSQLTranslationV1::TParsers Parsers;
};

TVector<NYql::TAstParseResult> TranslateStatementsSeparately(const TString& query) {
    TTranslationEnvironment env;

    return NSQLTranslationV1::SqlToAstStatements(
        env.Lexers,
        env.Parsers,
        query,
        env.Settings,
        /*warningRules=*/nullptr);
}

const NYql::TAstNode* FindListWithQuotedHead(const NYql::TAstNode* root, TStringBuf head) {
    if (!root->IsList()) {
        return nullptr;
    }
    if (root->GetChildrenCount()) {
        const auto* first = root->GetChild(0);
        if (first->IsList() && first->GetChildrenCount() == 2 &&
            first->GetChild(0)->IsAtom() && first->GetChild(0)->GetContent() == "quote" &&
            first->GetChild(1)->IsAtom() && first->GetChild(1)->GetContent() == head)
        {
            return root;
        }
    }
    for (ui32 i = 0; i < root->GetChildrenCount(); ++i) {
        if (const auto* found = FindListWithQuotedHead(root->GetChild(i), head)) {
            return found;
        }
    }
    return nullptr;
}

const NYql::TAstNode* Unquote(const NYql::TAstNode* node) {
    UNIT_ASSERT(node->IsList());
    UNIT_ASSERT_VALUES_EQUAL(node->GetChildrenCount(), 2);
    UNIT_ASSERT(node->GetChild(0)->IsAtom());
    UNIT_ASSERT_VALUES_EQUAL(node->GetChild(0)->GetContent(), "quote");
    return node->GetChild(1);
}

TStringBuf QuotedAtom(const NYql::TAstNode* node) {
    const auto* atom = Unquote(node);
    UNIT_ASSERT(atom->IsAtom());
    return atom->GetContent();
}

void AssertColumnDefaultSource(const NYql::TAstParseResult& result, TStringBuf column,
                               const TString& context, const TString& expression)
{
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
    const auto* columnNode = FindListWithQuotedHead(result.Root, column);
    UNIT_ASSERT_C(columnNode, column);
    if (const auto* constraint = FindListWithQuotedHead(columnNode, "setDefaultValue")) {
        UNIT_ASSERT_VALUES_EQUAL(constraint->GetChildrenCount(), 4);
        UNIT_ASSERT_VALUES_EQUAL(QuotedAtom(constraint->GetChild(2)), context);
        UNIT_ASSERT_VALUES_EQUAL(QuotedAtom(constraint->GetChild(3)), expression);
    } else {
        const auto* sourceConstraint = FindListWithQuotedHead(columnNode, "defaultSource");
        UNIT_ASSERT_C(sourceConstraint, column);
        UNIT_ASSERT_VALUES_EQUAL(sourceConstraint->GetChildrenCount(), 2);
        const auto* source = Unquote(sourceConstraint->GetChild(1));
        UNIT_ASSERT(source->IsList());
        UNIT_ASSERT_VALUES_EQUAL(source->GetChildrenCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(QuotedAtom(source->GetChild(0)), context);
        UNIT_ASSERT_VALUES_EQUAL(QuotedAtom(source->GetChild(1)), expression);
    }

    const auto recreated = SqlToYql(context + "SELECT " + expression + ";");
    UNIT_ASSERT_C(recreated.IsOk(), Err2Str(recreated));
}

class TCountingLexer: public NSQLTranslation::ILexer {
public:
    TCountingLexer(TPtr lexer, size_t& calls)
        : Lexer_(std::move(lexer))
        , Calls_(calls)
    {
    }

    bool Tokenize(const TString& query, const TString& queryName, const TTokenCallback& callback,
                  NYql::TIssues& issues, size_t maxErrors) override {
        ++Calls_;
        return Lexer_->Tokenize(query, queryName, callback, issues, maxErrors);
    }

private:
    TPtr Lexer_;
    size_t& Calls_;
};

class TCountingLexerFactory: public NSQLTranslation::ILexerFactory {
public:
    explicit TCountingLexerFactory(size_t& calls)
        : Calls_(calls)
    {
    }

    NSQLTranslation::ILexer::TPtr MakeLexer() const override {
        return MakeHolder<TCountingLexer>(MakeAntlr4LexerFactory()->MakeLexer(), Calls_);
    }

private:
    size_t& Calls_;
};

class TCountingParser: public NSQLTranslation::IParser {
public:
    TCountingParser(std::unique_ptr<IParser> parser, size_t& calls)
        : Parser_(std::move(parser))
        , Calls_(calls)
    {
    }

    google::protobuf::Message* Parse(const TString& query, const TString& queryName,
                                     NAST::IErrorCollector& errors, google::protobuf::Arena* arena) override {
        ++Calls_;
        return Parser_->Parse(query, queryName, errors, arena);
    }

private:
    std::unique_ptr<IParser> Parser_;
    size_t& Calls_;
};

class TCountingParserFactory: public NSQLTranslation::IParserFactory {
public:
    explicit TCountingParserFactory(size_t& calls)
        : Calls_(calls)
    {
    }

    std::unique_ptr<NSQLTranslation::IParser> MakeParser() const override {
        return std::make_unique<TCountingParser>(MakeAntlr4ParserFactory()->MakeParser(), Calls_);
    }

private:
    size_t& Calls_;
};

} // anonymous namespace

Y_UNIT_TEST(CreateTableDefaultSpaceBoundaries) {
    const std::array<TDefaultSourceTestCase, 9> testCases = {{
        {
            .Name = "family",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 101 + 1 FAMILY family_name,
                    PRIMARY KEY (key),
                    FAMILY family_name (DATA = "ssd")
                );
            )sql",
            .ExpectedExpression = "101 + 1",
        },
        {
            .Name = "null",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 102 + 2 NULL,
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "102 + 2",
        },
        {
            .Name = "compression",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 103 + 3 COMPRESSION(),
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "103 + 3",
        },
        {
            .Name = "encoding",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 104 + 4 ENCODING(OFF),
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "104 + 4",
        },
        {
            .Name = "generated",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 105 + 5 GENERATED ALWAYS AS (key + 1) STORED,
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "105 + 5",
        },
        {
            .Name = "as",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 106 + 6 AS (key + 2) VIRTUAL,
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "106 + 6",
        },
        {
            .Name = "next entry comma",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 DEFAULT 107 + 7,
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "107 + 7",
        },
        {
            .Name = "table right parenthesis",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    PRIMARY KEY (key),
                    value Int32 DEFAULT 108 + 8
                );
            )sql",
            .ExpectedExpression = "108 + 8",
        },
        {
            .Name = "trailing entry comma",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    PRIMARY KEY (key),
                    value Int32 DEFAULT 109 + 9,
                );
            )sql",
            .ExpectedExpression = "109 + 9",
        },
    }};

    for (const auto& testCase : testCases) {
        AssertDefaultSource(testCase);
    }
}

Y_UNIT_TEST(CreateTableDefaultCommaBoundaries) {
    const std::array<TDefaultSourceTestCase, 6> testCases = {{
        {
            .Name = "single option",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 (DEFAULT 201 + 1),
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "201 + 1",
        },
        {
            .Name = "first option",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 (DEFAULT 202 + 2, NOT NULL),
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "202 + 2",
        },
        {
            .Name = "middle option",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 (FAMILY family_name, DEFAULT 203 + 3, COMPRESSION()),
                    PRIMARY KEY (key),
                    FAMILY family_name (DATA = "ssd")
                );
            )sql",
            .ExpectedExpression = "203 + 3",
        },
        {
            .Name = "last option",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    value Int32 (ENCODING(OFF), DEFAULT 204 + 4),
                    PRIMARY KEY (key)
                );
            )sql",
            .ExpectedExpression = "204 + 4",
        },
        {
            .Name = "last column schema",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    PRIMARY KEY (key),
                    value Int32 (DEFAULT 205 + 5)
                );
            )sql",
            .ExpectedExpression = "205 + 5",
        },
        {
            .Name = "last column schema with trailing comma",
            .Query = R"sql(
                USE ydb;
                CREATE TABLE tbl (
                    key Uint32,
                    PRIMARY KEY (key),
                    value Int32 (DEFAULT 206 + 6),
                );
            )sql",
            .ExpectedExpression = "206 + 6",
        },
    }};

    for (const auto& testCase : testCases) {
        AssertDefaultSource(testCase);
    }
}

Y_UNIT_TEST(AlterTableAddColumnDefaultSpaceBoundaries) {
    const std::array<TDefaultSourceTestCase, 9> testCases = {{
        {
            .Name = "family",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 301 + 1 FAMILY family_name;
            )sql",
            .ExpectedExpression = "301 + 1",
        },
        {
            .Name = "null",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 302 + 2 NULL;
            )sql",
            .ExpectedExpression = "302 + 2",
        },
        {
            .Name = "compression",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 303 + 3 COMPRESSION();
            )sql",
            .ExpectedExpression = "303 + 3",
        },
        {
            .Name = "encoding",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 304 + 4 ENCODING(OFF);
            )sql",
            .ExpectedExpression = "304 + 4",
        },
        {
            .Name = "generated",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 305 + 5 GENERATED ALWAYS AS (key + 1) STORED;
            )sql",
            .ExpectedExpression = "305 + 5",
        },
        {
            .Name = "as",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 306 + 6 AS (key + 2) VIRTUAL;
            )sql",
            .ExpectedExpression = "306 + 6",
        },
        {
            .Name = "next action comma",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl
                    ADD COLUMN value Int32 DEFAULT 307 + 7,
                    DROP COLUMN old_value;
            )sql",
            .ExpectedExpression = "307 + 7",
        },
        {
            .Name = "semicolon",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 308 + 8;
            )sql",
            .ExpectedExpression = "308 + 8",
        },
        {
            .Name = "eof",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 309 + 9
            )sql",
            .ExpectedExpression = "309 + 9",
        },
    }};

    for (const auto& testCase : testCases) {
        AssertDefaultSource(testCase);
    }
}

Y_UNIT_TEST(AlterTableAddColumnDefaultCommaBoundaries) {
    const std::array<TDefaultSourceTestCase, 6> testCases = {{
        {
            .Name = "single option",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 (DEFAULT 401 + 1);
            )sql",
            .ExpectedExpression = "401 + 1",
        },
        {
            .Name = "first option",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 (DEFAULT 402 + 2, NOT NULL);
            )sql",
            .ExpectedExpression = "402 + 2",
        },
        {
            .Name = "middle option",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 (FAMILY family_name, DEFAULT 403 + 3, COMPRESSION());
            )sql",
            .ExpectedExpression = "403 + 3",
        },
        {
            .Name = "last option",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 (ENCODING(OFF), DEFAULT 404 + 4);
            )sql",
            .ExpectedExpression = "404 + 4",
        },
        {
            .Name = "next action comma",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl
                    ADD COLUMN value Int32 (DEFAULT 405 + 5),
                    DROP COLUMN old_value;
            )sql",
            .ExpectedExpression = "405 + 5",
        },
        {
            .Name = "eof",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ADD COLUMN value Int32 (DEFAULT 406 + 6)
            )sql",
            .ExpectedExpression = "406 + 6",
        },
    }};

    for (const auto& testCase : testCases) {
        AssertDefaultSource(testCase);
    }
}

Y_UNIT_TEST(AlterColumnSetDefaultBoundaries) {
    const std::array<TDefaultSourceTestCase, 5> testCases = {{
        {
            .Name = "next action comma",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl
                    ALTER COLUMN value SET DEFAULT 501 + 1,
                    DROP COLUMN old_value;
            )sql",
            .ExpectedExpression = "501 + 1",
        },
        {
            .Name = "trailing semicolon",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ALTER COLUMN value SET DEFAULT 502 + 2;
            )sql",
            .ExpectedExpression = "502 + 2",
        },
        {
            .Name = "following statement",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ALTER COLUMN value SET DEFAULT 503 + 3;
                COMMIT;
            )sql",
            .ExpectedExpression = "503 + 3",
        },
        {
            .Name = "eof",
            .Query = R"sql(
                USE ydb;
                ALTER TABLE tbl ALTER COLUMN value SET DEFAULT 504 + 4
            )sql",
            .ExpectedExpression = "504 + 4",
        },
        {
            .Name = "inline action end",
            .Query = R"sql(
                USE ydb;
                DO BEGIN
                    ALTER TABLE tbl ALTER COLUMN value SET DEFAULT 505 + 5
                END DO;
            )sql",
            .ExpectedExpression = "505 + 5",
        },
    }};

    for (const auto& testCase : testCases) {
        AssertDefaultSource(testCase);
    }
}

Y_UNIT_TEST(DefaultBeforeNotNullSpaceRequiresCommaList) {
    const std::array<TStringBuf, 2> queries = {{
        R"sql(
            USE ydb;
            CREATE TABLE tbl (
                key Uint32,
                value Int32 DEFAULT 601 + 1 NOT NULL,
                PRIMARY KEY (key)
            );
        )sql",
        R"sql(
            USE ydb;
            ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT 602 + 2 NOT NULL;
        )sql",
    }};

    for (const auto query : queries) {
        const auto res = SqlToYql(TString(query));
        UNIT_ASSERT(!res.IsOk());
        UNIT_ASSERT_STRING_CONTAINS(Err2Str(res), "recommended to use the syntax '(DEFAULT value, NOT NULL, ...)'");
    }
}

Y_UNIT_TEST(CreateTableDefaultKeepsExprTreeAndText) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Uint32 DEFAULT 5,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('default (Int32 '"5"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('defaultSource '('"USE ydb;\n" '"5"))#");
}

Y_UNIT_TEST(CreateTableDefaultArithmeticExpr) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT 1 + 2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
}

Y_UNIT_TEST(CreateTableDefaultParenthesizedExpr) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT (1 + 2) * 3,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"(1 + 2) * 3"))#");
}

Y_UNIT_TEST(CreateTableDefaultFunctionCall) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Uint64 DEFAULT RandomNumber(1),
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"RandomNumber(1)"))#");
}

Y_UNIT_TEST(CreateTableDefaultExtraWhitespaceStripped) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT     7    ,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"7"))#");
}

Y_UNIT_TEST(CreateTableDefaultPreservesUserSpelling) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT (1+2)*3,
            w Int32 DEFAULT 1  +  2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"(1+2)*3"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1  +  2"))#");
}

Y_UNIT_TEST(CreateTableDefaultNoNotNull) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT 1 + 2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
    UNIT_ASSERT(!program.Contains("not_null"));
}

Y_UNIT_TEST(CreateTableDefaultAndNotNullCommaExprBefore) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 (DEFAULT 1 + 2, NOT NULL),
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, "not_null");
}

Y_UNIT_TEST(CreateTableDefaultAndNotNullCommaExprAfter) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 (NOT NULL, DEFAULT 1 + 2),
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, "not_null");
}

Y_UNIT_TEST(CreateTableDefaultBeforeFamilySpace) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT 3*2 FAMILY family_name,
            PRIMARY KEY (key),
            FAMILY family_name (
                DATA = "ssd",
                COMPRESSION = "off"
            )
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"3*2")#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"family_name")#");
}

Y_UNIT_TEST(CreateTableNotNullSpaceDefault) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 NOT NULL DEFAULT 1 + 2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, "not_null");
}

Y_UNIT_TEST(CreateTableMultipleDefaultsIndependent) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            a Int32 DEFAULT 1 + 1,
            b Int32 DEFAULT 2 * 2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 1"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"2 * 2"))#");
}

Y_UNIT_TEST(CreateTableMultipleDefaultsMixedWithNotNull) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            a Int32 NOT NULL DEFAULT 1 + 1,
            b Int32 DEFAULT 2 * 2,
            c Int32 (DEFAULT 3 - 1, NOT NULL),
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 1"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"2 * 2"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"3 - 1"))#");
}

Y_UNIT_TEST(CreateTableDefaultWithPragmaContext) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        PRAGMA FlexibleTypes;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT 1 + 2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\nPRAGMA FlexibleTypes;\n" '"1 + 2"))#");
}

Y_UNIT_TEST(CreateTableDefaultWithNamedNodeContext) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        $factor = 10;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT $factor * 2,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n$factor = 10;\n" '"$factor * 2"))#");
}

Y_UNIT_TEST(AlterTableAddColumnDefault) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ADD COLUMN v Int32 DEFAULT 1 + 2;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
}

Y_UNIT_TEST(AlterTableAddColumnNotNullDefault) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ADD COLUMN v Int32 NOT NULL DEFAULT 5;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"5"))#");
    UNIT_ASSERT_STRING_CONTAINS(program, "not_null");
}

Y_UNIT_TEST(AlterTableAlterColumnSetDefaultExpr) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN v SET DEFAULT 1 + 2;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "setDefaultValue");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2"))#");
}

Y_UNIT_TEST(AlterTableAlterColumnSetDefaultKeepsExprTree) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN v SET DEFAULT 42;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('setDefaultValue (Int32 '"42")#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#((Int32 '"42") '"USE ydb;\n" '"42"))#");
}

Y_UNIT_TEST(AlterTableSetDefaultBeforeNextAction) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl
            ALTER COLUMN a SET DEFAULT 1 + 2,
            ALTER COLUMN b SET DEFAULT 3 * 4;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2")#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"3 * 4")#");
}

Y_UNIT_TEST(AlterTableSetDefaultBeforeFollowingStatement) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN v SET DEFAULT 1 + 2;
        COMMIT;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2")#");
}

Y_UNIT_TEST(AlterTableSetDefaultAtEof) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ALTER COLUMN v SET DEFAULT 1 + 2
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"1 + 2")#");
}

Y_UNIT_TEST(CreateTableDefaultWithNamedNodeRefNoRead) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        $base = 100;
        CREATE TABLE tbl (
            key Uint32,
            v Int32 DEFAULT $base + 1,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('default ("+MayWarn" namedexprnode0 (Int32 '"1"))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('defaultSource '('"USE ydb;\n$base = 100;\n" '"$base + 1")))#");
    UNIT_ASSERT(!program.Contains("Read!"));
}

Y_UNIT_TEST(AlterTableAddColumnDefaultWithNamedLambdaRefNoRead) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        $f = ($x) -> ($x * 2);
        ALTER TABLE tbl ADD COLUMN v Int32 DEFAULT $f(3);
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('default (Apply namedexprnode0 (Int32 '"3"))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('defaultSource '('"USE ydb;\n$f = ($x) -> ($x * 2);\n" '"$f(3)")))#");
    UNIT_ASSERT(!program.Contains("Read!"));
}

Y_UNIT_TEST(CreateTableDefaultWithSubqueryNamedNodeRefKeepsRead) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        $ids = (SELECT id FROM other);
        CREATE TABLE tbl (
            key Uint32,
            v Bool DEFAULT 1 IN $ids,
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#((Read! world (DataSource '"ydb" '"ydb") (Key '('table (String '"other"))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('default (SqlIn subquery0 (Int32 '"1") '('('tableSource) '('warnNoAnsi)))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('defaultSource '('"USE ydb;\n$ids = (SELECT id FROM other);\n" '"1 IN $ids")))#");
}

Y_UNIT_TEST(AlterTableAlterColumnSetDefaultWithSubqueryNamedNodeRefKeepsRead) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        $ids = (SELECT id FROM other);
        ALTER TABLE tbl ALTER COLUMN v SET DEFAULT 1 IN $ids;
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#((Read! world (DataSource '"ydb" '"ydb") (Key '('table (String '"other"))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('setDefaultValue (SqlIn subquery0 (Int32 '"1") '('('tableSource) '('warnNoAnsi))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n$ids = (SELECT id FROM other);\n" '"1 IN $ids")#");
}

Y_UNIT_TEST(CreateTableDefaultWithInlineSubqueryKeepsRead) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        CREATE TABLE tbl (
            key Uint32,
            v Bool DEFAULT 1 IN (SELECT id FROM other),
            PRIMARY KEY (key)
        );
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#((Read! world (DataSource '"ydb" '"ydb") (Key '('table (String '"other"))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('default (SqlIn subquery0 (Int32 '"1") '('('tableSource) '('warnNoAnsi)))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('defaultSource '('"USE ydb;\n" '"1 IN (SELECT id FROM other)")))#");
}

Y_UNIT_TEST(AlterTableAddColumnDefaultWithInlineSubqueryKeepsRead) {
    auto res = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE tbl ADD COLUMN v Bool DEFAULT 1 IN (SELECT id FROM other);
    )sql");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    const auto program = GetPrettyPrint(res);
    UNIT_ASSERT_STRING_CONTAINS(program, "addColumns");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#((Read! world (DataSource '"ydb" '"ydb") (Key '('table (String '"other"))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('default (SqlIn subquery0 (Int32 '"1") '('('tableSource) '('warnNoAnsi)))))#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('('defaultSource '('"USE ydb;\n" '"1 IN (SELECT id FROM other)")))#");
}

Y_UNIT_TEST(DefaultSourcePreservesCommentsAndUtf8) {
    const TString expression = "('Привет' /* запятая, DEFAULT */\r\n || 'мир')";
    const std::array<TString, 3> queries = {{
        "USE ydb; /* До DEFAULT: привет */ CREATE TABLE tbl (key Uint32, v Utf8 DEFAULT " + expression + ", PRIMARY KEY (key));",
        "USE ydb; /* До DEFAULT: привет */ ALTER TABLE tbl ADD COLUMN v Utf8 DEFAULT " + expression + ";",
        "USE ydb; /* До DEFAULT: привет */ ALTER TABLE tbl ALTER COLUMN v SET DEFAULT " + expression,
    }};

    for (const auto& query : queries) {
        const auto res = SqlToYql(query);
        UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
        UNIT_ASSERT_C(FindNodeByChildAtomContent(res.Root, 1, expression), GetPrettyPrint(res));
    }
}

Y_UNIT_TEST(DefaultSourceWithAnsiLexer) {
    const TString expression = "('it''s a DEFAULT, value' || 'x')";
    const auto res = SqlToYqlWithAnsiLexer(
        "USE ydb; ALTER TABLE \"table\" ADD COLUMN \"value\" String DEFAULT " + expression + ";");
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));
    UNIT_ASSERT_C(FindNodeByChildAtomContent(res.Root, 1, expression), GetPrettyPrint(res));
}

Y_UNIT_TEST(MultipleStatementsKeepIndependentDefaultSources) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        ALTER TABLE first_table ALTER COLUMN value SET DEFAULT 11 + 1;
        ALTER TABLE second_table ALTER COLUMN value SET DEFAULT 22 + 2;
    )sql");
    UNIT_ASSERT_C(result.IsOk(), Err2Str(result));

    const auto program = GetPrettyPrint(result);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"11 + 1")#");
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\n" '"22 + 2")#");
}

Y_UNIT_TEST(SplitTranslationKeepsMultipleDefaultSourcesIndependent) {
    const auto results = TranslateStatementsSeparately(R"sql(
        USE ydb;
        ALTER TABLE first_table ALTER COLUMN value SET DEFAULT 33 + 3;
        ALTER TABLE second_table ALTER COLUMN value SET DEFAULT 44 + 4;
    )sql");
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
    UNIT_ASSERT_C(results[0].IsOk(), Err2Str(results[0]));
    UNIT_ASSERT_C(results[1].IsOk(), Err2Str(results[1]));

    UNIT_ASSERT_STRING_CONTAINS(GetPrettyPrint(results[0]), R"#('"USE ydb;\n" '"33 + 3")#");
    UNIT_ASSERT_STRING_CONTAINS(GetPrettyPrint(results[1]), R"#('"USE ydb;\n" '"44 + 4")#");
}
Y_UNIT_TEST(SplitTranslationKeepsDefaultContextAfterOtherStatements) {
    const auto results = TranslateStatementsSeparately(R"sql(
        USE ydb;
        SELECT 1;
        PRAGMA FlexibleTypes;
        $base = 100;
        ALTER TABLE tbl ALTER COLUMN value SET DEFAULT $base + 1;
    )sql");
    UNIT_ASSERT_VALUES_EQUAL(results.size(), 2);
    UNIT_ASSERT_C(results[0].IsOk(), Err2Str(results[0]));
    UNIT_ASSERT_C(results[1].IsOk(), Err2Str(results[1]));

    const auto program = GetPrettyPrint(results[1]);
    UNIT_ASSERT_STRING_CONTAINS(program, R"#('"USE ydb;\nPRAGMA FlexibleTypes;\n$base = 100;\n" '"$base + 1")#");
}

Y_UNIT_TEST(NestedDefaultContextUsesLocalStatements) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        DO BEGIN
            $a = 1;
            ALTER TABLE tbl ALTER COLUMN value SET DEFAULT $a + 1;
        END DO;
    )sql");
    AssertColumnDefaultSource(result, "value", "USE ydb;\n$a = 1;\n", "$a + 1");
}

Y_UNIT_TEST(NestedDefaultContextSupportsMultipleLocals) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        DO BEGIN
            $a = 1;
            $b = 2;
            $c = 3;
            ALTER TABLE tbl ALTER COLUMN value SET DEFAULT $a + $b + $c;
        END DO;
    )sql");
    AssertColumnDefaultSource(result, "value", "USE ydb;\n$a = 1;\n$b = 2;\n$c = 3;\n", "$a + $b + $c");
}

Y_UNIT_TEST(DefaultContextRestoredAfterInlineAction) {
    const auto result = SqlToYql(R"sql(
        USE ydb;
        $a = 10;
        DO BEGIN
            $a = 20;
            DO BEGIN
                $a = 30;
                ALTER TABLE tbl ALTER COLUMN inner_value SET DEFAULT $a + 1;
            END DO;
            ALTER TABLE tbl ALTER COLUMN middle_value SET DEFAULT $a + 2;
        END DO;
        ALTER TABLE tbl ALTER COLUMN outer_value SET DEFAULT $a + 3;
    )sql");
    AssertColumnDefaultSource(result, "inner_value", "USE ydb;\n$a = 10;\n$a = 20;\n$a = 30;\n", "$a + 1");
    AssertColumnDefaultSource(result, "middle_value", "USE ydb;\n$a = 10;\n$a = 20;\n", "$a + 2");
    AssertColumnDefaultSource(result, "outer_value", "USE ydb;\n$a = 10;\n", "$a + 3");
}

Y_UNIT_TEST(DefaultContextPreservesCommentsAndAnsiLexer) {
    const TString context = "--!ansi_lexer\nUSE ydb;\n/* комментарий */ $a = 'it''s';\n";
    const auto result = SqlToYql(context + "ALTER TABLE tbl ALTER COLUMN value SET DEFAULT $a || 'it''s';");
    AssertColumnDefaultSource(result, "value", context, "$a || 'it''s'");
}

Y_UNIT_TEST(DefaultContextDoesNotPromoteTrailingComments) {
    for (const TString comment : {"--!ansi_lexer", "/* trailing comment */ --!ansi_lexer"}) {
        const auto result = SqlToYql("SELECT 1; " + comment + R"sql(
            USE ydb;
            ALTER TABLE tbl ALTER COLUMN value SET DEFAULT "hello";
        )sql");
        AssertColumnDefaultSource(result, "value", "USE ydb;\n", "\"hello\"");
    }
}

Y_UNIT_TEST(DefaultSourcePreservesTextAfterUtf8Bom) {
    const TString context = "USE ydb;\n/* локальная переменная */ $base = 10;\n";
    for (const TString statement : {
             "CREATE TABLE tbl (key Int32, value Int32 DEFAULT $base + 123, PRIMARY KEY (key));",
             "ALTER TABLE tbl ADD COLUMN value Int32 DEFAULT $base + 123;",
             "ALTER TABLE tbl ALTER COLUMN value SET DEFAULT $base + 123;"})
    {
        const auto result = SqlToYql("\xEF\xBB\xBF" + context + statement);
        AssertColumnDefaultSource(result, "value", context, "$base + 123");
    }
}

Y_UNIT_TEST(MultipleDefaultsDoNotRepeatParsingOrTokenization) {
    for (size_t defaults : {1, 8}) {
        size_t lexerCalls = 0;
        size_t parserCalls = 0;
        TTranslationEnvironment env;
        env.Lexers.Antlr4 = MakeIntrusive<TCountingLexerFactory>(lexerCalls);
        env.Parsers.Antlr4 = MakeIntrusive<TCountingParserFactory>(parserCalls);

        TStringBuilder query;
        query << "USE ydb; $base = 42; CREATE TABLE tbl (key Int32, ";
        for (size_t i = 0; i < defaults; ++i) {
            query << "c" << i << " Int32 DEFAULT $base + " << i << ", ";
        }
        query << "PRIMARY KEY (key));";
        const auto result = NSQLTranslationV1::SqlToYql(env.Lexers, env.Parsers, query, env.Settings);
        UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
        UNIT_ASSERT_VALUES_EQUAL(parserCalls, 1);
        // The same tokens supply SQL hints and every DEFAULT source fragment.
        UNIT_ASSERT_VALUES_EQUAL(lexerCalls, 1);
    }
}

Y_UNIT_TEST(SplitTranslationSharesSourceTokenization) {
    for (size_t defaults : {1, 8}) {
        size_t lexerCalls = 0;
        size_t parserCalls = 0;
        TTranslationEnvironment env;
        env.Lexers.Antlr4 = MakeIntrusive<TCountingLexerFactory>(lexerCalls);
        env.Parsers.Antlr4 = MakeIntrusive<TCountingParserFactory>(parserCalls);

        TStringBuilder query;
        query << "USE ydb; $base = 42;";
        for (size_t i = 0; i < defaults; ++i) {
            query << "ALTER TABLE tbl ALTER COLUMN c" << i << " SET DEFAULT $base + " << i << ";";
        }
        const auto results = NSQLTranslationV1::SqlToAstStatements(env.Lexers, env.Parsers, query, env.Settings, nullptr);
        UNIT_ASSERT_VALUES_EQUAL(results.size(), defaults);
        for (const auto& result : results) {
            UNIT_ASSERT_C(result.IsOk(), Err2Str(result));
        }
        UNIT_ASSERT_VALUES_EQUAL(parserCalls, 1);
        UNIT_ASSERT_VALUES_EQUAL(lexerCalls, 1);
        for (size_t i = 0; i < defaults; ++i) {
            AssertColumnDefaultSource(results[i], "c" + ToString(i), "USE ydb;\n$base = 42;\n", "$base + " + ToString(i));
        }
    }
}

} // Y_UNIT_TEST_SUITE(ColumnDefaultExpr)
