#include <ydb/core/grpc_services/local_rpc/local_rpc.h>
#include <ydb/public/api/protos/ydb_scheme.pb.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NGRpcService {

Y_UNIT_TEST_SUITE(LocalGrpcRequestContext) {
    Y_UNIT_TEST(PreservesOriginalPeerAcrossWrappers) {
        using TRequest = Ydb::Scheme::MakeDirectoryRequest;
        using TResponse = Ydb::Scheme::MakeDirectoryResponse;
        using TRpc = TGrpcRequestOperationCall<TRequest, TResponse>;
        using TContext = NRpcService::TLocalRpcCtx<TRpc, std::function<void(const TResponse&)>>;
        class TGrpcContext : public NLocalGrpc::TContext<TRequest, TResponse> {
        public:
            using NLocalGrpc::TContext<TRequest, TResponse>::TContext;

            TString GetRpcMethodName() const override {
                return "Ydb.Scheme.V1.SchemeService/MakeDirectory";
            }
        };
        for (const TString& peer : {TString("ipv4:192.0.2.1:2135"), TString("ipv6:[2001:db8::1]:2135"),
                TString("2001:db8::2"), TString()}) {
            auto original = std::make_shared<TContext>(TRequest{}, [](const TResponse&) {}, TContext::TSettings{
                .PeerName = peer,
            });
            auto firstContext = MakeIntrusive<TGrpcContext>(TRequest{}, original, [](const TResponse&) {});
            auto firstRequest = std::make_shared<TRpc>(firstContext.Get(),
                [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
            auto secondContext = MakeIntrusive<TGrpcContext>(TRequest{}, firstRequest, [](const TResponse&) {});
            TRpc secondRequest(secondContext.Get(), [](std::unique_ptr<IRequestOpCtx>, const IFacilityProvider&) {});
            UNIT_ASSERT_VALUES_EQUAL_C(secondRequest.GetPeerName(), peer, peer);
        }
    }
}

} // namespace NKikimr::NGRpcService
