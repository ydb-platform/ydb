#include "kicli.h"

#include <ydb/public/lib/deprecated/client/msgbus_client.h>

#include <ydb/core/protos/node_broker.pb.h>

namespace NKikimr {
namespace NClient {

TRegistrationResult::TRegistrationResult(const TResult& result)
    : TResult(result)
{
}

const NKikimrClient::TNodeRegistrationResponse& TRegistrationResult::Record() const
{
    return GetResponse<NMsgBusProxy::TBusNodeRegistrationResponse>().Record;
}

bool TRegistrationResult::IsSuccess() const
{
    return (GetError().Success()
            && Record().GetStatus().GetCode() == NKikimrNodeBroker::TStatus::OK);
}

TString TRegistrationResult::GetErrorMessage() const
{
    if (!GetError().Success())
        return GetError().GetMessage();
    return Record().GetStatus().GetReason();
}

ui32 TRegistrationResult::GetNodeId() const
{
    return Record().GetNodeId();
}

NActors::TScopeId TRegistrationResult::GetScopeId() const
{
    const auto& record = Record();
    if (record.HasScopeTabletId() && record.HasScopePathId()) {
        return {record.GetScopeTabletId(), record.GetScopePathId()};
    }
    return {};
}

TNodeRegistrant::TNodeRegistrant(TKikimr& kikimr)
    : Kikimr(&kikimr)
{
}

TRegistrationResult
TNodeRegistrant::SyncRegisterNode(const TString& domainPath,
                                  const TString& host,
                                  ui16 port,
                                  const TString& address,
                                  const TString& resolveHost,
                                  const NActors::TNodeLocation& location,
                                  bool fixedNodeId,
                                  TMaybe<TString> path) const
{
    auto future = Kikimr->RegisterNode(domainPath, host, port, address, resolveHost, location, fixedNodeId, path);
    auto result = future.GetValue(TDuration::Max());
    return TRegistrationResult(result);
}

} // NClient
} // NKikimr
