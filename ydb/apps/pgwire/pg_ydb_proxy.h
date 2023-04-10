#pragma once
#include <library/cpp/actors/core/actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

namespace NPGW {

NActors::IActor* CreateDatabaseProxy(const NYdb::TDriverConfig& driverConfig);

}
