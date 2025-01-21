#include "ydb_cluster.h"

#include "ydb_dynamic_config.h"
#include <ydb/public/sdk/cpp/client/ydb_bsconfig/ydb_storage_config.h>

using namespace NKikimr;

namespace NYdb::NConsoleClient::NCluster {

TCommandCluster::TCommandCluster()
    : TClientCommandTree("cluster", {}, "Cluster-wide administration")
{
    AddCommand(std::make_unique<TCommandClusterBootstrap>());
    AddCommand(std::make_unique<NDynamicConfig::TCommandConfig>());
}

TCommandClusterBootstrap::TCommandClusterBootstrap()
    : TYdbCommand("bootstrap", {}, "Bootstrap automatically-assembled cluster")
{}

void TCommandClusterBootstrap::Config(TConfig& config) {
    TYdbCommand::Config(config);
    config.Opts->AddLongOption("uuid", "Self-assembly UUID").RequiredArgument("STRING").StoreResult(&SelfAssemblyUUID);
    config.SetFreeArgsNum(0);
    config.AllowEmptyDatabase = true;
}

void TCommandClusterBootstrap::Parse(TConfig& config) {
    TClientCommand::Parse(config);
}

int TCommandClusterBootstrap::Run(TConfig& config) {
    auto driver = std::make_unique<NYdb::TDriver>(CreateDriver(config));
    NYdb::NStorageConfig::TStorageConfigClient client(*driver);
    auto result = client.BootstrapCluster(SelfAssemblyUUID).GetValueSync();
    ThrowOnError(result);
    return EXIT_SUCCESS;
}

} // namespace NYdb::NConsoleClient::NCluster
