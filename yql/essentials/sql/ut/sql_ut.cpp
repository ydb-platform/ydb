#include <library/cpp/testing/unittest/registar.h>

#include <yql/essentials/sql/sql.h>

#include <google/protobuf/any.pb.h>

namespace {

struct TCallState {
    bool TextToAst = false;
    bool TextToMessage = false;
    bool MakeLexer = false;
    bool TextAndMessageToAst = false;
    bool TextToManyAst = false;
};

class TMockTranslator final: public NSQLTranslation::ITranslator {
public:
    explicit TMockTranslator(TCallState& calls)
        : Calls_(calls)
    {
    }

    NSQLTranslation::ILexer::TPtr MakeLexer(const NSQLTranslation::TTranslationSettings&) final {
        Calls_.MakeLexer = true;
        return {};
    }

    NYql::TAstParseResult TextToAst(const TString&, const NSQLTranslation::TTranslationSettings&,
                                    NYql::TWarningRules*, NYql::TStmtParseInfo*) final {
        Calls_.TextToAst = true;
        return {};
    }

    google::protobuf::Message* TextToMessage(const TString&, const TString&, NYql::TIssues&, size_t,
                                             const NSQLTranslation::TTranslationSettings&) final {
        Calls_.TextToMessage = true;
        return nullptr;
    }

    NYql::TAstParseResult TextAndMessageToAst(const TString&, const google::protobuf::Message&,
                                              const NSQLTranslation::TSQLHints&,
                                              const NSQLTranslation::TTranslationSettings&) final {
        Calls_.TextAndMessageToAst = true;
        return {};
    }

    TVector<NYql::TAstParseResult> TextToManyAst(const TString&, const NSQLTranslation::TTranslationSettings&,
                                                 NYql::TWarningRules*, TVector<NYql::TStmtParseInfo>*) final {
        Calls_.TextToManyAst = true;
        return {};
    }

private:
    TCallState& Calls_;
};

NSQLTranslation::TTranslators MakeTranslators(TCallState& calls) {
    NSQLTranslation::TTranslatorsRegistry registry = {
        {"mock", [&calls] { return MakeIntrusive<TMockTranslator>(calls); }},
    };
    return {nullptr, nullptr, nullptr, std::move(registry)};
}

} // namespace

Y_UNIT_TEST_SUITE(TSqlTranslation) {
Y_UNIT_TEST(UsesCustomSyntaxTranslator) {
    TCallState calls;
    NSQLTranslation::TTranslatorsRegistry translatorsRegistry = {
        {"mock", [&calls] { return MakeIntrusive<TMockTranslator>(calls); }},
    };
    NSQLTranslation::TTranslators translators(nullptr, nullptr, nullptr, std::move(translatorsRegistry));
    NSQLTranslation::TTranslationSettings settings;

    NSQLTranslation::SqlToYql(translators, "--!syntax_mock\nSELECT 1;", settings);

    UNIT_ASSERT(calls.TextToAst);
}

Y_UNIT_TEST(RejectsUnknownCustomSyntax) {
    NSQLTranslation::TTranslators translators(nullptr, nullptr, nullptr);
    NSQLTranslation::TTranslationSettings settings;

    const auto result = NSQLTranslation::SqlToYql(translators, "--!syntax_missing\nSELECT 1;", settings);

    UNIT_ASSERT(!result.IsOk());
    UNIT_ASSERT_C(result.Issues.ToString().Contains("Unknown syntax: missing"), result.Issues.ToString());
}

Y_UNIT_TEST(UsesCustomSyntaxForSqlAst) {
    TCallState calls;
    auto translators = MakeTranslators(calls);
    NSQLTranslation::TTranslationSettings settings;
    NYql::TIssues issues;

    NSQLTranslation::SqlAST(translators, "--!syntax_mock\nSELECT 1;", "query", issues, 1, settings);

    UNIT_ASSERT(calls.TextToMessage);
}

Y_UNIT_TEST(UsesCustomSyntaxForSqlLexer) {
    TCallState calls;
    auto translators = MakeTranslators(calls);
    NSQLTranslation::TTranslationSettings settings;
    NYql::TIssues issues;

    NSQLTranslation::SqlLexer(translators, "--!syntax_mock\nSELECT 1;", issues, settings);

    UNIT_ASSERT(calls.MakeLexer);
}

Y_UNIT_TEST(UsesCustomSyntaxForSqlAstToYql) {
    TCallState calls;
    auto translators = MakeTranslators(calls);
    NSQLTranslation::TTranslationSettings settings;
    settings.Syntax = "mock";
    google::protobuf::Any protoAst;
    NSQLTranslation::TSQLHints hints;

    NSQLTranslation::SqlASTToYql(translators, "SELECT 1;", protoAst, hints, settings);

    UNIT_ASSERT(calls.TextAndMessageToAst);
}

Y_UNIT_TEST(UsesCustomSyntaxForSqlToAstStatements) {
    TCallState calls;
    auto translators = MakeTranslators(calls);
    NSQLTranslation::TTranslationSettings settings;

    NSQLTranslation::SqlToAstStatements(translators, "--!syntax_mock\nSELECT 1;", settings);

    UNIT_ASSERT(calls.TextToManyAst);
}
} // Y_UNIT_TEST_SUITE(TSqlTranslation)
