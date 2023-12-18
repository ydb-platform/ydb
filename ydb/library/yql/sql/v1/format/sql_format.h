#pragma once

#include <ydb/library/yql/public/issue/yql_issue.h>
#include <ydb/library/yql/sql/settings/translation_settings.h>

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

ISqlFormatter::TPtr MakeSqlFormatter(const NSQLTranslation::TTranslationSettings& settings = {});

// insert spaces and comments between each tokens
TString MutateQuery(const TString& query, const NSQLTranslation::TTranslationSettings& settings = {});

bool SqlFormatSimple(const TString& query, TString& formattedQuery, TString& error);

THashSet<TString> GetKeywords();

}
