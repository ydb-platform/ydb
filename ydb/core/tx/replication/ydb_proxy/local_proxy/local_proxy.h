#pragma once

#include <ydb/core/grpc_services/base/base.h>
#include <ydb/core/tx/replication/ydb_proxy/ydb_proxy.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>

namespace NKikimr::NReplication {

using namespace NActors;

class TLocalProxyActor
    : public TActorBootstrapped<TLocalProxyActor>
    , public NGRpcService::IFacilityProvider
{
public:
    explicit TLocalProxyActor(const TString& database);

    void Bootstrap();

    ui64 GetChannelBufferSize() const override;
    TActorId RegisterActor(IActor* actor) const override;

private:
    void Handle(TEvYdbProxy::TEvAlterTopicRequest::TPtr& ev);
    void Handle(TEvYdbProxy::TEvCommitOffsetRequest::TPtr& ev);
    void Handle(TEvYdbProxy::TEvCreateTopicReaderRequest::TPtr& ev);
    void Handle(TEvYdbProxy::TEvDescribePathRequest::TPtr& ev);
    void Handle(TEvYdbProxy::TEvDescribeTableRequest::TPtr& ev);
    void Handle(TEvYdbProxy::TEvDescribeTopicRequest::TPtr& ev);

    STATEFN(StateWork);


private:
    const TString Database;
    TString LogPrefix;
};

}
