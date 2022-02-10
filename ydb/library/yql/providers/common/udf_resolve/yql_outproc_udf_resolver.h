#pragma once

#include <ydb/library/yql/core/yql_udf_resolver.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>
#include <ydb/library/yql/providers/common/proto/udf_resolver.pb.h>

#include <util/generic/map.h>
#include <util/generic/string.h>

namespace NYql {
namespace NCommon {

void LoadSystemModulePaths(
        const TString& resolverPath,
        const TString& dir,
        NKikimr::NMiniKQL::TUdfModulePathsMap* paths);

IUdfResolver::TPtr CreateOutProcUdfResolver(
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TFileStoragePtr& fileStorage,
    const TString& resolverPath,
    const TString& user,
    const TString& group,
    bool filterSysCalls,
    const TString& udfDependencyStubPath,
    const TMap<TString, TString>& path2md5 = {});

} // namespace NCommon
} // namespace NYql
