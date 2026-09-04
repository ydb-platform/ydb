#pragma once

#include <yql/essentials/sql/v1/ide/core/environment.h>
#include <yql/essentials/sql/v1/ide/completion/core/input.h>
#include <yql/essentials/sql/v1/ide/completion/core/name.h>
#include <yql/essentials/utils/meta/reflection.h>

#include <util/generic/ptr.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NSQLComplete {

using NSQLPureAST::TEnvironment;

struct TClusterContext {
    TString Provider;
    TString Name;

    friend bool operator==(const TClusterContext& lhs, const TClusterContext& rhs) = default;
};

struct TFunctionContext {
    TString Name;
    size_t ArgumentNumber = 0;
    TMaybe<TString> Arg0 = Nothing();
    TMaybe<TString> Arg1 = Nothing();
    TMaybe<TClusterContext> Cluster = Nothing();

    friend bool operator==(const TFunctionContext& lhs, const TFunctionContext& rhs) = default;
};

// TODO(YQL-19747): Try to refactor to use Map/Set data structures
struct TColumnContext {
    TVector<TAliased<TTableId>> Tables;
    TVector<TColumnId> Columns;
    THashMap<TString, THashSet<TString>> WithoutByTableAlias;

    [[nodiscard]] bool IsAsterisk() const;
    TColumnContext ExtractAliased(TMaybe<TStringBuf> alias);
    TColumnContext Renamed(TStringBuf alias) &&;

    friend bool operator==(const TColumnContext& lhs, const TColumnContext& rhs) = default;
    friend TColumnContext operator|(TColumnContext lhs, TColumnContext rhs);

    static TColumnContext Asterisk();
};

struct TGlobalContext {
    TMaybe<TClusterContext> Use;
    TVector<TString> Names;
    TMaybe<TFunctionContext> EnclosingFunction;
    TMaybe<TColumnContext> Column;
};

class IGlobalAnalysis {
public:
    using TPtr = THolder<IGlobalAnalysis>;

    virtual ~IGlobalAnalysis() = default;
    virtual TGlobalContext Analyze(TCompletionInput input, TEnvironment env) const = 0;
};

IGlobalAnalysis::TPtr MakeGlobalAnalysis();

} // namespace NSQLComplete

namespace NYql::NReflection {

YQL_DEFINE_REFLECTING(NSQLComplete::TClusterContext, (Provider)(Name));
YQL_DEFINE_REFLECTING(NSQLComplete::TFunctionContext, (Name)(ArgumentNumber)(Arg0)(Arg1)(Cluster));
YQL_DEFINE_REFLECTING(NSQLComplete::TColumnContext, (Tables)(Columns)(WithoutByTableAlias));

} // namespace NYql::NReflection
