#include "proto_parser.h"
#include "range.h"

#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/utils/string/trim_indent.h>

#include <library/cpp/testing/unittest/registar.h>

#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

#include <util/string/builder.h>

#include <limits>

namespace {

using NSQLTranslationV1::TTokenIndexRange;

TTokenIndexRange GetTokenIndexRangeReflecting(const google::protobuf::Message& node) {
    const auto* descriptor = node.GetDescriptor();
    const auto* reflection = node.GetReflection();
    const auto* start = descriptor->FindFieldByName("StartTokenIndex");
    const auto* end = descriptor->FindFieldByName("EndTokenIndex");
    UNIT_ASSERT_C(start && end &&
                      start->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT32 &&
                      end->cpp_type() == google::protobuf::FieldDescriptor::CPPTYPE_UINT32,
                  descriptor->full_name());
    return {
        reflection->GetUInt32(node, start),
        reflection->GetUInt32(node, end),
    };
}

using TRules = TVector<const google::protobuf::Message*>;

void CollectRules(const google::protobuf::Message& node, TRules& rules);

void CollectChildren(const google::protobuf::Message& node, TRules& rules) {
    const auto* descriptor = node.GetDescriptor();
    const auto* reflection = node.GetReflection();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        if (field->is_repeated()) {
            for (int j = 0; j < reflection->FieldSize(node, field); ++j) {
                CollectRules(reflection->GetRepeatedMessage(node, field, j), rules);
            }
        } else if (reflection->HasField(node, field)) {
            CollectRules(reflection->GetMessage(node, field), rules);
        }
    }
}

void CollectRules(const google::protobuf::Message& node, TRules& rules) {
    const TString name = node.GetDescriptor()->name();
    if (name == "TToken") {
        return;
    }
    const auto range = GetTokenIndexRangeReflecting(node);
    // Check every syntax node, but show only nonempty grammar rules.
    if (name.StartsWith("TRule_") && !range.IsEmpty()) {
        rules.push_back(&node);
    } else {
        CollectChildren(node, rules);
    }
}

TString TextAt(const TTokenIndexRange& range, const NSQLTranslation::TParsedTokenList& tokens) {
    UNIT_ASSERT(!range.IsEmpty());
    UNIT_ASSERT_LE(range.Start, range.End);
    UNIT_ASSERT_LT(range.End, tokens.size());
    TString text;
    for (ui32 i = range.Start; i <= range.End; ++i) {
        if (i + 1 != tokens.size()) { // The final token is EOF.
            text += tokens[i].Content;
        }
    }
    return text;
}

const google::protobuf::Message* FindRule(const google::protobuf::Message& node, TStringBuf name) {
    if (TStringBuf(node.GetDescriptor()->name()) == name) {
        return &node;
    }
    const auto* reflection = node.GetReflection();
    const auto* descriptor = node.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const auto* field = descriptor->field(i);
        if (field->cpp_type() != google::protobuf::FieldDescriptor::CPPTYPE_MESSAGE) {
            continue;
        }
        const int count = field->is_repeated() ? reflection->FieldSize(node, field) : reflection->HasField(node, field);
        for (int j = 0; j < count; ++j) {
            const auto& child = field->is_repeated()
                                    ? reflection->GetRepeatedMessage(node, field, j)
                                    : reflection->GetMessage(node, field);
            if (const auto* found = FindRule(child, name)) {
                return found;
            }
        }
    }
    return nullptr;
}

void PrintRule(const google::protobuf::Message& rule, const NSQLTranslation::TParsedTokenList& tokens, size_t depth, TString& output) {
    const auto range = GetTokenIndexRangeReflecting(rule);
    TRules children;
    CollectChildren(rule, children);

    const auto* last = &rule;
    while (children.size() == 1 && GetTokenIndexRangeReflecting(*children.front()) == range) {
        last = children.front();
        children.clear();
        CollectChildren(*last, children);
    }

    const TStringBuf prefix = "TRule_";
    TString path = rule.GetDescriptor()->name().substr(prefix.size());
    if (last != &rule) {
        path += "..";
        path += last->GetDescriptor()->name().substr(prefix.size());
    }

    output += TString(depth * 2, ' ');
    output += TStringBuilder() << '(' << path << ") " << TextAt(range, tokens) << '\n';
    for (const auto* child : children) {
        PrintRule(*child, tokens, depth + 1, output);
    }
}

const google::protobuf::Message* ParseTree(const TString& query, google::protobuf::Arena& arena) {
    NSQLTranslationV1::TParsers parsers;
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(false, false, Nothing());
    NYql::TIssues issues;
    const auto* tree = NSQLTranslationV1::SqlAST(
        parsers, query, "", issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS, false, &arena);
    UNIT_ASSERT_C(tree, issues.ToOneLineString());
    return tree;
}

TString PrintTree(const TString& query) {
    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    auto lexer = NSQLTranslationV1::MakeLexer(lexers, false);

    NYql::TIssues issues;
    NSQLTranslation::TParsedTokenList tokens;
    UNIT_ASSERT_C(
        NSQLTranslation::Tokenize(*lexer, query, "", tokens, issues, NSQLTranslation::SQL_MAX_PARSER_ERRORS),
        issues.ToOneLineString());

    google::protobuf::Arena arena;
    const auto* tree = ParseTree(query, arena);

    const auto* file = tree->GetDescriptor()->file();
    UNIT_ASSERT(!file->FindMessageTypeByName("TTokenRange"));
    const auto* token = file->FindMessageTypeByName("TToken");
    UNIT_ASSERT(token);
    UNIT_ASSERT(!token->FindFieldByName("Range"));

    TString output;
    TRules rules;
    CollectRules(*tree, rules);
    for (const auto* rule : rules) {
        PrintRule(*rule, tokens, 0, output);
    }
    UNIT_ASSERT(!output.empty());
    output.pop_back();
    return output;
}

} // namespace

Y_UNIT_TEST_SUITE(ProtoParserRangeTests) {

Y_UNIT_TEST(EmptyRangeAtTokenZero) {
    const TTokenIndexRange range{0, std::numeric_limits<ui32>::max()};
    UNIT_ASSERT(range.IsEmpty());
}

Y_UNIT_TEST(EmptyOptionalProduction) {
    google::protobuf::Arena arena;
    const auto* tree = ParseTree("SELECT 1", arena);
    const auto* optional = FindRule(*tree, "TRule_opt_set_quantifier");
    UNIT_ASSERT(optional);
    const auto range = GetTokenIndexRangeReflecting(*optional);
    UNIT_ASSERT_GT(range.Start, 0);
    UNIT_ASSERT_VALUES_EQUAL(range.End, range.Start - 1);
}

Y_UNIT_TEST(SimpleSelect) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT 1"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT 1
          (sql_stmt..select_core) SELECT 1
            (result_column..integer) 1
    )"));
}

Y_UNIT_TEST(HiddenTokens) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT /*x*/ 1;"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT /*x*/ 1;
          (sql_stmt..select_core) SELECT /*x*/ 1
            (result_column..integer) 1
    )"));
}

Y_UNIT_TEST(SurroundingWhitespace) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree(" SELECT 1 "),
                             "(sql_query..sql_stmt_list) SELECT 1\x20\n"
                             "  (sql_stmt..select_core) SELECT 1\n"
                             "    (result_column..integer) 1");
}

Y_UNIT_TEST(RepeatedStatements) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT 1; SELECT 2"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT 1; SELECT 2
          (sql_stmt..select_core) SELECT 1
            (result_column..integer) 1
          (sql_stmt..select_core) SELECT 2
            (result_column..integer) 2
    )"));
}

Y_UNIT_TEST(ArithmeticExpression) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT 1 + 2"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT 1 + 2
          (sql_stmt..select_core) SELECT 1 + 2
            (result_column..bit_subexpr) 1 + 2
              (add_subexpr..integer) 1
              (add_subexpr..integer) 2
    )"));
}

Y_UNIT_TEST(FunctionInvocation) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT Abs(1)"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT Abs(1)
          (sql_stmt..select_core) SELECT Abs(1)
            (result_column..unary_casual_subexpr) Abs(1)
              (id_expr..identifier) Abs
              (unary_subexpr_suffix..invoke_expr) (1)
                (named_expr_list..integer) 1
    )"));
}

Y_UNIT_TEST(InlineSubquery) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT (SELECT 1)"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT (SELECT 1)
          (sql_stmt..select_core) SELECT (SELECT 1)
            (result_column..smart_parenthesis) (SELECT 1)
              (select_subexpr..select_core) SELECT 1
                (result_column..integer) 1
    )"));
}

Y_UNIT_TEST(FromClause) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT x FROM (SELECT 1 AS x)"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT x FROM (SELECT 1 AS x)
          (sql_stmt..select_core) SELECT x FROM (SELECT 1 AS x)
            (result_column..identifier) x
            (join_source..single_source) (SELECT 1 AS x)
              (select_stmt..select_core) SELECT 1 AS x
                (result_column) 1 AS x
                  (expr..integer) 1
                  (an_id_or_type..identifier) x
    )"));
}

Y_UNIT_TEST(ParenthesizedExpression) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT (1 + 2) * 3"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT (1 + 2) * 3
          (sql_stmt..select_core) SELECT (1 + 2) * 3
            (result_column..add_subexpr) (1 + 2) * 3
              (mul_subexpr..smart_parenthesis) (1 + 2)
                (select_subexpr..bit_subexpr) 1 + 2
                  (add_subexpr..integer) 1
                  (add_subexpr..integer) 2
              (mul_subexpr..integer) 3
    )"));
}

Y_UNIT_TEST(OperatorChain) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT 1 - 2 - 3"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT 1 - 2 - 3
          (sql_stmt..select_core) SELECT 1 - 2 - 3
            (result_column..bit_subexpr) 1 - 2 - 3
              (add_subexpr..integer) 1
              (add_subexpr..integer) 2
              (add_subexpr..integer) 3
    )"));
}

Y_UNIT_TEST(OperatorPrecedence) {
    UNIT_ASSERT_VALUES_EQUAL(PrintTree("SELECT 1 + 2 * 3"), NYql::TrimIndent(R"(
        (sql_query..sql_stmt_list) SELECT 1 + 2 * 3
          (sql_stmt..select_core) SELECT 1 + 2 * 3
            (result_column..bit_subexpr) 1 + 2 * 3
              (add_subexpr..integer) 1
              (add_subexpr) 2 * 3
                (mul_subexpr..integer) 2
                (mul_subexpr..integer) 3
    )"));
}

} // Y_UNIT_TEST_SUITE(ProtoParserRangeTests)
