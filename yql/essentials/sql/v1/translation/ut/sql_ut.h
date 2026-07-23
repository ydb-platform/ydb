#pragma once

#include <yql/essentials/ast/yql_ast.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/parser/lexer_common/lexer.h>

#include <yql/essentials/core/langver/feature.gen.h>

#include <yql/essentials/utils/string/trim_indent.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NSQLTranslationV1 {

enum class EDebugOutput {
    None,
    ToCerr,
};

class TWordCountHive: public TMap<TString, unsigned> {
public:
    TWordCountHive(std::initializer_list<TString> strings);
    TWordCountHive(std::initializer_list<std::pair<const TString, unsigned>> list);
};

using TVerifyLineFunc = std::function<void(const TString& word, const TString& line)>;

constexpr ui32 PRETTY_FLAGS =
    NYql::TAstPrintFlags::PerLine |
    NYql::TAstPrintFlags::ShortQuote |
    NYql::TAstPrintFlags::AdaptArbitraryContent;

TString Err2Str(const NYql::TAstParseResult& res, EDebugOutput debug = EDebugOutput::None);

NYql::TAstParseResult SqlToYqlWithMode(
    const TString& query,
    NSQLTranslation::ESqlMode mode = NSQLTranslation::ESqlMode::QUERY,
    size_t maxErrors = 10,
    const TString& provider = {},
    EDebugOutput debug = EDebugOutput::None,
    bool ansiLexer = false,
    NSQLTranslation::TTranslationSettings settings = {});

NYql::TAstParseResult SqlToYql(
    const TString& query,
    size_t maxErrors = 10,
    const TString& provider = {},
    EDebugOutput debug = EDebugOutput::None);

NYql::TAstParseResult SqlToYqlWithSettings(
    const TString& query, const NSQLTranslation::TTranslationSettings& settings);

void ExpectFailWithError(const TString& query, const TString& error);

void ExpectFailWithError(
    const TString& query, const TString& error, const NSQLTranslation::TTranslationSettings& settings);

void ExpectFailWithFuzzyError(const TString& query, const TString& errorRegex);

NYql::TAstParseResult SqlToYqlWithAnsiLexer(
    const TString& query,
    size_t maxErrors = 10,
    const TString& provider = {},
    EDebugOutput debug = EDebugOutput::None);

void ExpectFailWithErrorForAnsiLexer(const TString& query, const TString& error);

TString GetPrettyPrint(const NYql::TAstParseResult& res);

TString Quote(const char* str);

TString VerifyProgram(
    const NYql::TAstParseResult& res,
    TWordCountHive& wordCounter,
    TVerifyLineFunc verifyLine = TVerifyLineFunc());

void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints, TMaybe<bool> ansi);

void VerifySqlInHints(const TString& query, const THashSet<TString>& expectedHints);

NSQLTranslation::TTranslationSettings GetSettingsWithS3Binding(const TString& name);

const NYql::TAstNode* FindNodeByChildAtomContent(
    const NYql::TAstNode* root, uint32_t childIndex, TStringBuf name);

TString ToString(const NSQLTranslation::TParsedTokenList& tokens);

} // namespace NSQLTranslationV1
