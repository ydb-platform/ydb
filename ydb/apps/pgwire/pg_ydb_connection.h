#pragma once
#include <library/cpp/actors/core/actor.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/settings.h>

namespace NPGW {

NActors::IActor* CreateConnection(NYdb::TDriver driver, std::unordered_map<TString, TString> params);

}
