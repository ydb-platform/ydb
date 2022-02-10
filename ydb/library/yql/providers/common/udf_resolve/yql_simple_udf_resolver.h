#pragma once

#include <ydb/library/yql/core/yql_type_annotation.h>
#include <ydb/library/yql/core/file_storage/file_storage.h>

#include <ydb/library/yql/minikql/mkql_alloc.h>
#include <ydb/library/yql/minikql/mkql_function_registry.h>

namespace NYql {
namespace NCommon {

IUdfResolver::TPtr CreateSimpleUdfResolver(
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TFileStoragePtr& fileStorage = {}, bool useFakeMD5 = false);

bool LoadFunctionsMetadata(const TVector<IUdfResolver::TFunction*>& functions,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
    TExprContext& ctx);

} // namespace NCommon
} // namespace NYql
