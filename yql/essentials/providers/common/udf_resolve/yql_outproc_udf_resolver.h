#pragma once

#include <yql/essentials/core/yql_udf_resolver.h>
#include <yql/essentials/core/file_storage/file_storage.h>
#include <yql/essentials/minikql/mkql_function_registry.h>
#include <yql/essentials/providers/common/proto/udf_resolver.pb.h>

#include <util/generic/map.h>
#include <util/generic/maybe.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql::NCommon {

struct TOutProcUdfResolverOptions {
    struct TPortoConfig {
        TString ProxyBinaryPath;
        TVector<TString> Layers;
    };

    TString ResolverPath;
    bool FilterSysCalls = false;
    TMaybe<TString> User;
    TMaybe<TString> Group;
    TMaybe<TPortoConfig> PortoConfig;
    TMaybe<TString> UdfDependencyStubPath;
};

void LoadSystemModulePaths(
    const TOutProcUdfResolverOptions& options,
    const TString& dir,
    NKikimr::NMiniKQL::TUdfModulePathsMap* paths);

IUdfResolver::TPtr CreateOutProcUdfResolver(
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TFileStoragePtr& fileStorage,
    TOutProcUdfResolverOptions options,
    const TMap<TString, TString>& path2md5 = {});

} // namespace NYql::NCommon
