#include "kicli.h"

#include <ydb/public/lib/deprecated/client/msgbus_client.h>

#include <ydb/core/protos/node_broker.pb.h>
#include <ydb/core/protos/console_base.pb.h>
#include <ydb/core/protos/console_config.pb.h>

namespace NKikimr {
namespace NClient {

TConfigurationResult::TConfigurationResult(const TResult& result)
    : TResult(result)
{
}

const NKikimrClient::TConsoleResponse& TConfigurationResult::Record() const
{
    return GetResponse<NMsgBusProxy::TBusConsoleResponse>().Record;
}

bool TConfigurationResult::IsSuccess() const
{
    return (GetError().Success()
            && Record().GetStatus().GetCode() == Ydb::StatusIds::SUCCESS);
}

TString TConfigurationResult::GetErrorMessage() const
{
    if (!GetError().Success())
        return GetError().GetMessage();
    return Record().GetStatus().GetReason();
}

const NKikimrConfig::TAppConfig &TConfigurationResult::GetConfig() const
{
    return Record().GetGetNodeConfigResponse().GetConfig();
}

bool TConfigurationResult::HasMainYamlConfig() const
{
    return Record().GetGetNodeConfigResponse().HasMainYamlConfig();
}

const TString& TConfigurationResult::GetMainYamlConfig() const
{
    return Record().GetGetNodeConfigResponse().GetMainYamlConfig();
}

TMap<ui64, TString> TConfigurationResult::GetVolatileYamlConfigs() const
{
    TMap<ui64, TString> volatileConfigs;
    for (auto &item : Record().GetGetNodeConfigResponse().GetVolatileConfigs()) {
       volatileConfigs.emplace(item.GetId(), item.GetConfig());
    }
    return volatileConfigs;
}

bool TConfigurationResult::HasDatabaseYamlConfig() const
{
    return Record().GetGetNodeConfigResponse().HasDatabaseYamlConfig();
}

const TString& TConfigurationResult::GetDatabaseYamlConfig() const
{
    return Record().GetGetNodeConfigResponse().GetDatabaseYamlConfig();
}

TNodeConfigurator::TNodeConfigurator(TKikimr& kikimr)
    : Kikimr(&kikimr)
{
}

TConfigurationResult TNodeConfigurator::SyncGetNodeConfig(ui32 nodeId,
                                                          const TString &host,
                                                          const TString &tenant,
                                                          const TString &nodeType,
                                                          const TString& domain,
                                                          const TString& token,
                                                          bool serveYaml,
                                                          ui64 version) const
{
    auto future = Kikimr->GetNodeConfig(nodeId, host, tenant, nodeType, domain, token, serveYaml, version);
    auto result = future.GetValue(TDuration::Max());
    return TConfigurationResult(result);
}

} // NClient
} // NKikimr
