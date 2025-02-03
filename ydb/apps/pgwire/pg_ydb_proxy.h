#pragma once
#include <ydb/library/actors/core/actor.h>
#include <ydb-cpp-sdk/client/driver/driver.h>

namespace NPGW {

NActors::IActor* CreateDatabaseProxy(const NYdb::TDriverConfig& driverConfig);

}
