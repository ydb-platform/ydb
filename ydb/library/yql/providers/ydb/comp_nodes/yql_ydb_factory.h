#pragma once

#include <ydb/library/yql/minikql/computation/mkql_computation_node.h>
#include <ydb-cpp-sdk/client/driver/driver.h>

namespace NYql {

NKikimr::NMiniKQL::TComputationNodeFactory GetDqYdbFactory(NYdb::TDriver driver);

}

