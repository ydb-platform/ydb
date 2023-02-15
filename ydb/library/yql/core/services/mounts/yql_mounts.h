#pragma once

#include <ydb/library/yql/core/user_data/yql_user_data.h>
#include <ydb/library/yql/core/yql_type_annotation.h>

namespace NYql {

void LoadYqlDefaultMounts(TUserDataTable& userData);

bool GetYqlDefaultModuleResolver(
        TExprContext& ctx,
        IModuleResolver::TPtr& moduleResolver,
        const THashMap<TString, TString>& clusterMapping = {},
        bool optimizeLibraries = true);

bool GetYqlDefaultModuleResolverWithContext(
    IModuleResolver::TPtr& moduleResolver,
    const THashMap<TString, TString>& clusterMapping = {},
    bool optimizeLibraries = true);

TUserDataTable GetYqlModuleResolver(
        TExprContext& ctx,
        IModuleResolver::TPtr& moduleResolver,
        const TVector<NUserData::TUserData>& userData,
        const THashMap<TString, TString>& clusterMapping,
        const THashSet<TString>& sqlFlags,
        bool optimizeLibraries = true);

} // namespace NYql
