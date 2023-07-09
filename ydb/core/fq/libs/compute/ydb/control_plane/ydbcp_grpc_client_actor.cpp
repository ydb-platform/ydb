#include <ydb/core/fq/libs/compute/ydb/events/events.h>

#include <ydb/library/services/services.pb.h>

#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/ycloud/api/events.h>
#include <ydb/library/ycloud/impl/grpc_service_client.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/event.h>
#include <library/cpp/actors/core/hfunc.h>

namespace NFq {

class TYdbcpGrpcServiceActor : public NActors::TActor<TYdbcpGrpcServiceActor> {
public:
    using TBase = NActors::TActor<TYdbcpGrpcServiceActor>;
    TYdbcpGrpcServiceActor(const NCloud::TGrpcClientSettings&,
                           const NYdb::TCredentialsProviderPtr&)
        : TBase(&TYdbcpGrpcServiceActor::StateFunc)
    {}

    STRICT_STFUNC(StateFunc,
        hFunc(TEvYdbCompute::TEvCreateDatabaseRequest, Handle);
    )

    void Handle(TEvYdbCompute::TEvCreateDatabaseRequest::TPtr& ev) {
        auto forwardResponse = std::make_unique<TEvYdbCompute::TEvCreateDatabaseResponse>();
        forwardResponse->Issues.AddIssue("Ydbcp grpc client hasn't supported yet");
        Send(ev->Sender, forwardResponse.release(), 0, ev->Cookie);
    }
};

std::unique_ptr<NActors::IActor> CreateYdbcpGrpcClientActor(const NCloud::TGrpcClientSettings& settings, const NYdb::TCredentialsProviderPtr& credentialsProvider) {
    return std::make_unique<TYdbcpGrpcServiceActor>(settings, credentialsProvider);
}

}
