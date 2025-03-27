#pragma once

#include <yql/essentials/core/yql_type_annotation.h>
#include <yql/essentials/core/file_storage/file_storage.h>

#include <yql/essentials/minikql/mkql_alloc.h>
#include <yql/essentials/minikql/mkql_function_registry.h>

namespace NYql {
namespace NCommon {

IUdfResolver::TPtr CreateSimpleUdfResolver(
    const NKikimr::NMiniKQL::IFunctionRegistry* functionRegistry,
    const TFileStoragePtr& fileStorage = {}, bool useFakeMD5 = false);

bool LoadFunctionsMetadata(const TVector<IUdfResolver::TFunction*>& functions,
    const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    NUdf::ITypeInfoHelper::TPtr typeInfoHelper,
    TExprContext& ctx, NUdf::ELogLevel logLevel);

} // namespace NCommon
} // namespace NYql
