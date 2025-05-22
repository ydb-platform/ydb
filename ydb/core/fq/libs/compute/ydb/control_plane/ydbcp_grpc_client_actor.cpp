#include <ydb/core/fq/libs/compute/ydb/events/events.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/grpc/actor_client/grpc_service_client.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/event.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NFq {

class TYdbcpGrpcServiceActor : public NActors::TActor<TYdbcpGrpcServiceActor> {
public:
    using TBase = NActors::TActor<TYdbcpGrpcServiceActor>;
    TYdbcpGrpcServiceActor(const NGrpcActorClient::TGrpcClientSettings&,
                           const NYdb::TCredentialsProviderPtr&)
        : TBase(&TYdbcpGrpcServiceActor::StateFunc)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
        hFunc(TEvYdbCompute::TEvListDatabasesRequest, Handle);
    )

    void Handle(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev) {
        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvCreateDatabaseResponse>();
        forwardResponse->Issues.AddIssue("Ydbcp grpc client hasn't supported yet");
        Send(ev->Sender, forwardResponse.release(), 0, ev->Cookie);
    }

    void Handle(TEvYdbCompute::TEvListDatabasesRequest::TPtr& ev) {
        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvListDatabasesResponse>();
        forwardResponse->Issues.AddIssue("Ydbcp grpc client hasn't supported yet");
        Send(ev->Sender, forwardResponse.release(), 0, ev->Cookie);
    }
};

std::unique_ptr<NActors::IActor> CreateYdbcpGrpcClientActor(const NGrpcActorClient::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider) {
    return std::make_unique<TYdbcpGrpcServiceActor>(settings, credentialsProvider);
}

}
