#include "kicli.h" 
 
#include <ydb/public/lib/deprecated/client/msgbus_client.h>
 
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
 
TNodeConfigurator::TNodeConfigurator(TKikimr& kikimr) 
    : Kikimr(&kikimr) 
{ 
} 
 
TConfigurationResult TNodeConfigurator::SyncGetNodeConfig(ui32 nodeId, 
                                                          const TString &host, 
                                                          const TString &tenant, 
                                                          const TString &nodeType, 
                                                          const TString& domain,
                                                          const TString& token) const
{ 
    auto future = Kikimr->GetNodeConfig(nodeId, host, tenant, nodeType, domain, token);
    auto result = future.GetValue(TDuration::Max()); 
    return TConfigurationResult(result); 
} 
 
} // NClient 
} // NKikimr 
