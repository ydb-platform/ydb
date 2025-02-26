#pragma once

#include <yql/essentials/public/issue/yql_issue.h>
#include <yql/essentials/sql/settings/translation_settings.h>
#include <yql/essentials/sql/v1/lexer/lexer.h>
#include <yql/essentials/sql/v1/proto_parser/proto_parser.h>

#include <util/generic/string.h>

namespace NSQLFormat {

constexpr ui32 OneIndent = 4;

enum class EFormatMode {
    Pretty,
    Obfuscate
};

class ISqlFormatter {
public:
    using TPtr = THolder<ISqlFormatter>;

    virtual bool Format(const TString& query, TString& formattedQuery, NYql::TIssues& issues,
        EFormatMode mode = EFormatMode::Pretty) = 0;
    virtual ~ISqlFormatter() = default;
};

//FIXME remove
ISqlFormatter::TPtr MakeSqlFormatter(const NSQLTranslation::TTranslationSettings& settings = {});

ISqlFormatter::TPtr MakeSqlFormatter(const NSQLTranslationV1::TLexers& lexers,
    const NSQLTranslationV1::TParsers& parsers,
    const NSQLTranslation::TTranslationSettings& settings = {});

// insert spaces and comments between each tokens
TString MutateQuery(const NSQLTranslationV1::TLexers& lexers, const TString& query, const NSQLTranslation::TTranslationSettings& settings = {});

bool SqlFormatSimple(const NSQLTranslationV1::TLexers& lexers,
    const NSQLTranslationV1::TParsers& parsers, const TString& query, TString& formattedQuery, TString& error);

THashSet<TString> GetKeywords();

}
