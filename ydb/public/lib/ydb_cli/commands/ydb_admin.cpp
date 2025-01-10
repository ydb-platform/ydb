#include "ydb_admin.h"

#include "ydb_dynamic_config.h"
#include "ydb_storage_config.h"
#include "ydb_cluster.h"

namespace NYdb {
namespace NConsoleClient {

class TCommandNode : public TClientCommandTree {
public:
    TCommandNode()
        : TClientCommandTree("node", {}, "Node-wide administration")
    {}
};

class TCommandDatabase : public TClientCommandTree {
public:
    TCommandDatabase()
        : TClientCommandTree("database", {}, "Database-wide administration")
    {}
};

TCommandAdmin::TCommandAdmin()
    : TClientCommandTree("admin", {}, "Administrative cluster operations")
{
    MarkDangerous();
    UseOnlyExplicitProfile();
    AddHiddenCommand(std::make_unique<NDynamicConfig::TCommandConfig>());
    AddHiddenCommand(std::make_unique<NDynamicConfig::TCommandVolatileConfig>());
    AddHiddenCommand(std::make_unique<NStorageConfig::TCommandStorageConfig>());
    AddCommand(std::make_unique<NCluster::TCommandCluster>());
    AddCommand(std::make_unique<TCommandNode>());
    AddCommand(std::make_unique<TCommandDatabase>());
}

}
}
