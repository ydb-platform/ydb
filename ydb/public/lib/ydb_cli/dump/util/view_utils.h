#pragma once

#include <util/generic/string.h>
#include <util/stream/str.h>

namespace NSQLTranslationV1 {
    struct TLexers;
}

namespace NSQLTranslation {
    struct TTranslationSettings;
}

namespace NYql {
    class TIssues;
}

namespace NYdb::NDump {

struct TViewQuerySplit {
    TString ContextRecreation;
    TString Select;

    TViewQuerySplit() = default;
    TViewQuerySplit(const TVector<TString>& statements);
};

bool SplitViewQuery(const TString& query, TViewQuerySplit& split, NYql::TIssues& issues);
bool SplitViewQuery(
    const TString& query, const NSQLTranslationV1::TLexers& lexers, const NSQLTranslation::TTranslationSettings& translationSettings,
    TViewQuerySplit& split, NYql::TIssues& issues
);

TString BuildCreateViewQuery(
    const TString& name, const TString& dbPath, const TString& viewQuery, const TString& database, const TString& backupRoot,
    NYql::TIssues& issues
);

bool RewriteCreateViewQuery(TString& query, const TString& restoreRoot, bool restoreRootIsDatabase,
    const TString& dbPath, NYql::TIssues& issues
);

} // NYdb::NDump
