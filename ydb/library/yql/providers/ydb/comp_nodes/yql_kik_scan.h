#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb/library/yql/minikql/mkql_node.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NYql::NDqs {

template<bool Async>
NKikimr::NMiniKQL::IComputationNode* WrapKikScan(NKikimr::NMiniKQL::TCallable& callable, const NKikimr::NMiniKQL::TComputationNodeFactoryContext& ctx, const NYdb::TDriver& driver);

} // NYql
