#pragma once

#include <yql/essentials/minikql/computation/mkql_computation_node.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>

namespace NYql {

NKikimr::NMiniKQL::TComputationNodeFactory GetDqYdbFactory(NYdb::TDriver driver);

}

