#pragma once

#include "yql_yt_file_services.h"

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/computation/mkql_computation_node_impl.h>
#include <ydb/library/yql/public/udf/udf_value.h>

#include <library/cpp/yson/node/node.h>

namespace NYql::NFile {

NKikimr::NMiniKQL::TComputationNodeFactory GetYtFileFactory(const TYtFileServices::TPtr& services);
NKikimr::NMiniKQL::TComputationNodeFactory GetYtFileFullFactory(const TYtFileServices::TPtr& services);
TVector<TString> GetFileWriteResult(const NKikimr::NMiniKQL::TTypeEnvironment& env, const NKikimr::NMiniKQL::IFunctionRegistry& functionRegistry,
    NKikimr::NMiniKQL::TComputationContext& ctx, const NKikimr::NUdf::TUnboxedValue& value, const NYT::TNode& outSpecs);

} // NYql::NFile
