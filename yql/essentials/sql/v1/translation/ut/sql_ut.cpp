#include "sql_ut.h"

#include <yql/essentials/sql/sql.h>
#include <yql/essentials/sql/v1/translation/sql.h>

#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4/lexer.h>
#include <yql/essentials/sql/v1/lexer/antlr4_ansi/lexer.h>

#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4/proto_parser.h>
#include <yql/essentials/sql/v1/proto_parser/antlr4_ansi/proto_parser.h>

#include <yql/essentials/providers/common/provider/yql_provider_names.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/regex/pcre/pcre.h>

#include <util/string/split.h>

namespace NSQLTranslationV1 {

namespace {

void AstBfs(NYql::TAstNode const* root, std::function<bool(NYql::TAstNode const*)> visitor) {
    std::deque<NYql::TAstNode const*> wishList{root};
    std::unordered_set<NYql::TAstNode const*> visited;
    while (!wishList.empty()) {
        auto v = wishList.front();
        wishList.pop_front();
        if (!visitor(v)) {
            return;
        }
        visited.insert(v);
        if (v->IsList()) {
            for (ui32 i = 0; i != v->GetChildrenCount(); ++i) {
                auto child = v->GetChild(i);
                if (visited.find(child) == visited.cend()) {
                    wishList.push_back(child);
                }
            }
        }
    }
}

} // namespace

TWordCountHive::TWordCountHive(std::initializer_list<TString> strings) {
    for (auto& str : strings) {
        emplace(str, 0);
    }
}

TWordCountHive::TWordCountHive(std::initializer_list<std::pair<const TString, unsigned>> list)
    : TMap(list)
{
}

TString Err2Str(const NYql::TAstParseResult& res, EDebugOutput debug) {
    TStringStream s;
    res.Issues.PrintTo(s);

    if (debug == EDebugOutput::ToCerr) {
        Cerr << s.Str() << Endl;
    }
    return s.Str();
}

NYql::TAstParseResult SqlToYqlWithMode(
    const TString& query,
    NSQLTranslation::ESqlMode mode,
    size_t maxErrors,
    const TString& provider,
    EDebugOutput debug,
    bool ansiLexer,
    NSQLTranslation::TTranslationSettings settings)
{
    google::protobuf::Arena arena;
    const auto service = provider ? provider : TString(NYql::YtProviderName);
    const TString cluster = "plato";
    settings.ClusterMapping[cluster] = service;
    settings.ClusterMapping["hahn"] = NYql::YtProviderName;
    settings.ClusterMapping["mon"] = NYql::SolomonProviderName;
    settings.ClusterMapping["rtmr"] = NYql::RtmrProviderName;
    settings.ClusterMapping["ydb"] = NYql::YdbProviderName;
    settings.MaxErrors = maxErrors;
    settings.Mode = mode;
    settings.Arena = &arena;
    settings.AnsiLexer = ansiLexer;
    settings.SyntaxVersion = 1;

    NSQLTranslationV1::TLexers lexers;
    lexers.Antlr4 = NSQLTranslationV1::MakeAntlr4LexerFactory();
    NSQLTranslationV1::TParsers parsers;
    lexers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiLexerFactory();
    parsers.Antlr4 = NSQLTranslationV1::MakeAntlr4ParserFactory(
        /*isAmbiguityError=*/true,
        /*isAmbiguityDebugging=*/false);
    parsers.Antlr4Ansi = NSQLTranslationV1::MakeAntlr4AnsiParserFactory(
        /*isAmbiguityError=*/true,
        /*isAmbiguityDebugging=*/false);

    NSQLTranslation::TTranslators translators(
        nullptr,
        NSQLTranslationV1::MakeTranslator(lexers, parsers),
        nullptr);

    auto res = SqlToYql(translators, query, settings);
    if (debug == EDebugOutput::ToCerr) {
        Err2Str(res, debug);
    }

    return res;
}

NYql::TAstParseResult SqlToYql(
    const TString& query,
    size_t maxErrors,
    const TString& provider,
    EDebugOutput debug)
{
    return SqlToYqlWithMode(query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug);
}

NYql::TAstParseResult SqlToYqlWithSettings(
    const TString& query,
    const NSQLTranslation::TTranslationSettings& settings)
{
    return SqlToYqlWithMode(
        query,
        NSQLTranslation::ESqlMode::QUERY,
        /*maxErrors=*/10,
        /*provider=*/{},
        EDebugOutput::None,
        /*ansiLexer=*/false,
        settings);
}

void ExpectFailWithError(const TString& query, const TString& error) {
    NYql::TAstParseResult res = SqlToYql(query);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

void ExpectFailWithError(
    const TString& query,
    const TString& error,
    const NSQLTranslation::TTranslationSettings& settings)
{
    NYql::TAstParseResult res = SqlToYqlWithSettings(query, settings);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

void ExpectFailWithFuzzyError(const TString& query, const TString& errorRegex) {
    NYql::TAstParseResult res = SqlToYql(query);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT(NPcre::TPcre<char>(errorRegex.c_str()).Matches(Err2Str(res)));
}

NYql::TAstParseResult SqlToYqlWithAnsiLexer(
    const TString& query,
    size_t maxErrors,
    const TString& provider,
    EDebugOutput debug)
{
    bool ansiLexer = true;
    return SqlToYqlWithMode(
        query, NSQLTranslation::ESqlMode::QUERY, maxErrors, provider, debug, ansiLexer);
}

void ExpectFailWithErrorForAnsiLexer(const TString& query, const TString& error) {
    NYql::TAstParseResult res = SqlToYqlWithAnsiLexer(query);

    UNIT_ASSERT(!res.IsOk());
    UNIT_ASSERT_NO_DIFF(Err2Str(res), error);
}

TString GetPrettyPrint(const NYql::TAstParseResult& res) {
    TStringStream yqlProgram;
    res.Root->PrettyPrintTo(yqlProgram, NYql::TAstPrintFlags::PerLine | NYql::TAstPrintFlags::ShortQuote);
    return yqlProgram.Str();
}

TString Quote(const char* str) {
    return TStringBuilder() << "'\"" << str << "\"";
}

TString VerifyProgram(
    const NYql::TAstParseResult& res,
    TWordCountHive& wordCounter,
    TVerifyLineFunc verifyLine)
{
    const auto program = GetPrettyPrint(res);
    TVector<TString> yqlProgram;
    Split(program, "\n", yqlProgram);
    for (const auto& line : yqlProgram) {
        for (auto& counterIter : wordCounter) {
            const auto& word = counterIter.first;
            auto pos = line.find(word);
            while (pos != TString::npos) {
                ++counterIter.second;
                if (verifyLine) {
                    verifyLine(word, line);
                }
                pos = line.find(word, pos + word.length());
            }
        }
    }
    return program;
}

void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints, TMaybe<bool> ansi) {
    TString pragma;
    if (ansi.Defined()) {
        pragma = *ansi ? "PRAGMA AnsiInForEmptyOrNullableItemsCollections;"
                       : "PRAGMA DisableAnsiInForEmptyOrNullableItemsCollections;";
    }

    NYql::TAstParseResult res = SqlToYql(pragma + query);
    UNIT_ASSERT_C(res.IsOk(), Err2Str(res));

    TVerifyLineFunc verifyLine = [&](const TString& word, const TString& line) {
        Y_UNUSED(word);
        if (!ansi.Defined()) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('warnNoAnsi)"));
        } else if (*ansi) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find("'('ansi)"));
        }
        for (auto& hint : expectedHints) {
            UNIT_ASSERT_VALUES_UNEQUAL(TString::npos, line.find(hint));
        }
    };
    TWordCountHive elementStat = {{TString("SqlIn"), 0}};
    VerifyProgram(res, elementStat, verifyLine);
}

void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints) {
    VerifySqlInHints(query, expectedHints, false);
    VerifySqlInHints(query, expectedHints, true);
}

NSQLTranslation::TTranslationSettings GetSettingsWithS3Binding(const TString& name) {
    NSQLTranslation::TTranslationSettings settings;
    NSQLTranslation::TTableBindingSettings bindSettings;
    bindSettings.ClusterType = "s3";
    bindSettings.Settings["cluster"] = "cluster";
    bindSettings.Settings["path"] = "path";
    bindSettings.Settings["format"] = "format";
    bindSettings.Settings["compression"] = "ccompression";
    bindSettings.Settings["bar"] = "1";
    // schema is not validated in this test but should be valid YSON text
    bindSettings.Settings["schema"] = R"__("[
                        "StructType";
                        [
                            [
                                "key";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "subkey";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ];
                            [
                                "value";
                                [
                                    "DataType";
                                    "String"
                                ]
                            ]
    ]])__";
    bindSettings.Settings["partitioned_by"] = R"(["key", "subkey"])";
    settings.Bindings[name] = bindSettings;
    return settings;
}

const NYql::TAstNode* FindNodeByChildAtomContent(
    const NYql::TAstNode* root,
    uint32_t childIndex,
    TStringBuf name)
{
    const NYql::TAstNode* result = nullptr;
    AstBfs(root, [&result, childIndex, name](auto v) {
        if (v->IsList() && v->GetChildrenCount() > childIndex &&
            v->GetChild(childIndex)->IsAtom() && v->GetChild(childIndex)->GetContent() == name) {
            result = v;
            return false;
        }
        return true; });
    return result;
}

TString ToString(const NSQLTranslation::TParsedTokenList& tokens) {
    TStringBuilder reconstructedQuery;
    for (const auto& token : tokens) {
        if (token.Name == "WS" || token.Name == "EOF") {
            continue;
        }
        if (!reconstructedQuery.empty()) {
            reconstructedQuery << ' ';
        }
        reconstructedQuery << token.Content;
    }
    return reconstructedQuery;
}

} // namespace NSQLTranslationV1
