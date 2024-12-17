#include "ydb_admin.h"

#include "ydb_dynamic_config.h"
#include "ydb_storage_config.h"
#include "ydb_cluster.h"

namespace NYdb {
namespace NConsoleClient {

TCommandAdmin::TCommandAdmin()
    : TClientCommandTree("admin", {}, "Administrative cluster operations")
{
    AddCommand(std::make_unique<NDynamicConfig::TCommandConfig>());
    AddCommand(std::make_unique<NDynamicConfig::TCommandVolatileConfig>());
    AddCommand(std::make_unique<NStorageConfig::TCommandStorageConfig>());
    AddCommand(std::make_unique<NCluster::TCommandCluster>());
}

}
}
