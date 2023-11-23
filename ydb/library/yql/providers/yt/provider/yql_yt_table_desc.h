#pragma once

#include <ydb/library/yql/ast/yql_expr.h>
#include <ydb/library/yql/core/url_lister/interface/url_lister_manager.h>
#include <ydb/library/yql/core/yql_udf_resolver.h>

#include <library/cpp/random_provider/random_provider.h>

#include <util/system/types.h>
#include <util/generic/flags.h>
#include <util/generic/string.h>
#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/hash.h>
#include <util/generic/ptr.h>

#include <utility>

namespace NYql {

enum class TYtTableIntent: ui32 {
    Read        = 1 << 0,
    View        = 1 << 1, // Read via view
    Override    = 1 << 2,
    Append      = 1 << 3,
    Create      = 1 << 4, // Reserved. Not implemented yet
    Drop        = 1 << 5,
    Flush       = 1 << 6, // Untransactional write
};

Y_DECLARE_FLAGS(TYtTableIntents, TYtTableIntent);
Y_DECLARE_OPERATORS_FOR_FLAGS(TYtTableIntents);

inline bool HasReadIntents(TYtTableIntents intents) {
    return intents & (TYtTableIntent::Read | TYtTableIntent::View);
}

inline bool HasModifyIntents(TYtTableIntents intents) {
    return intents & (TYtTableIntent::Override | TYtTableIntent::Append | TYtTableIntent::Drop | TYtTableIntent::Flush);
}

inline bool HasExclusiveModifyIntents(TYtTableIntents intents) {
    return intents & (TYtTableIntent::Override | TYtTableIntent::Drop | TYtTableIntent::Flush);
}

struct TYtViewDescription {
    TString Sql;
    ui16 SyntaxVersion = 1;
    TExprNode::TPtr CompiledSql; // contains Read! to self/self_raw tables
    const TTypeAnnotationNode* RowType = nullptr; // Filled only if scheme requested

    bool Fill(const TString& provider, const TString& cluster, const TString& sql, ui16 syntaxVersion, TExprContext& ctx,
        IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider, 
        bool enableViewIsolation, IUdfResolver::TPtr udfResolver);
    void CleanupCompiledSQL();
};

struct TYtTableDescriptionBase {
    const TTypeAnnotationNode* RawRowType = nullptr;
    const TTypeAnnotationNode* RowType = nullptr; // may be customized by UDF if scheme requested
    TExprNode::TPtr UdfApplyLambda; // convert table row by UDF
    bool HasUdfApply = false;
    TMap<TString, TYtViewDescription> Views;
    TMaybe<TYtViewDescription> View;
    TMap<TString, TString> ProtoFields;
    TYtTableIntents Intents;
    ui32 InferSchemaRows = 0;
    bool ForceInferSchema = false;
    bool FailOnInvalidSchema = true;
    bool HasWriteLock = false;
    bool IgnoreTypeV3 = false;

    bool Fill(const TString& provider, const TString& cluster, const TString& table, const TStructExprType* type,
        const TString& viewSql, ui16 syntaxVersion, const THashMap<TString, TString>& metaAttrs, TExprContext& ctx,
        IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider,
        bool enableViewIsolation, IUdfResolver::TPtr udfResolver);
    void CleanupCompiledSQL();
    bool FillViews(const TString& provider, const TString& cluster, const TString& table, const THashMap<TString, TString>& metaAttrs,
        TExprContext& ctx, IModuleResolver* moduleResolver, IUrlListerManager* urlListerManager, IRandomProvider& randomProvider,
        bool enableViewIsolation, IUdfResolver::TPtr udfResolver);
};

}
