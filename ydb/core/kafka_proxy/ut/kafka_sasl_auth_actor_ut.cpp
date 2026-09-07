#include <ydb/core/base/appdata.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/kafka_proxy/actors/kafka_sasl_auth_actor.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/network/sock.h>

namespace NKafka::NTests {
namespace {

using namespace NKikimr;
using TNavigate = NSchemeCache::TSchemeCacheNavigate;

class TFakeSchemeCacheActor : public NActors::TActor<TFakeSchemeCacheActor> {
public:
    TFakeSchemeCacheActor()
        : TActor(&TFakeSchemeCacheActor::StateWork)
    {
    }

    STRICT_STFUNC(StateWork,
        hFunc(TEvTxProxySchemeCache::TEvNavigateKeySet, Handle);)

private:
    void Handle(TEvTxProxySchemeCache::TEvNavigateKeySet::TPtr& ev) {
        auto request = std::move(ev->Get()->Request);
        for (auto& entry : request->ResultSet) {
            entry.Status = TNavigate::EStatus::Ok;
            entry.Kind = TNavigate::EKind::KindExtSubdomain;
            entry.Path = {"Root"};
            entry.DomainInfo = MakeIntrusive<NSchemeCache::TDomainInfo>(TPathId{1, 1}, TPathId{1, 1});
        }
        Send(ev->Sender, new TEvTxProxySchemeCache::TEvNavigateKeySetResult(std::move(request)));
    }
};

} // namespace

Y_UNIT_TEST_SUITE(KafkaSaslAuthActor) {
    Y_UNIT_TEST(ForwardsRequestIdToTicketParser) {
        TAppPrepare app;
        app.AddDomain(TDomainsInfo::TDomain::ConstructEmptyDomain("Root", 1).Release());

        NActors::TTestBasicRuntime runtime(1, false);
        runtime.Initialize(app.Unwrap());
        runtime.GetAppData().TenantName = "/Root";

        auto schemeCacheId = runtime.Register(new TFakeSchemeCacheActor());
        runtime.RegisterService(MakeSchemeCacheID(), schemeCacheId);

        NKikimrConfig::TKafkaProxyConfig config;
        auto context = std::make_shared<TContext>(config);
        auto edge = runtime.AllocateEdgeActor();
        runtime.RegisterService(MakeTicketParserID(), edge);
        context->ConnectionId = edge;
        context->SaslMechanism = "MTLS";

        const TString requestId = "kafka-request-id";
        auto address = std::make_shared<TSockAddrInet>("127.0.0.1", 9092);
        auto actorId = runtime.Register(CreateKafkaSaslAuthActor(context, address, requestId));

        TDispatchOptions bootstrapOptions;
        bootstrapOptions.FinalEvents.emplace_back(TEvents::TSystem::Bootstrap, 1);
        UNIT_ASSERT(runtime.DispatchEvents(bootstrapOptions, TDuration::Seconds(5)));
        runtime.Send(actorId, edge, new TEvKafka::TEvMtlsAuthRequest("client-certificate"));

        auto authorizeTicket = runtime.GrabEdgeEvent<TEvTicketParser::TEvAuthorizeTicket>(edge, TDuration::Seconds(5));
        UNIT_ASSERT(authorizeTicket);
        UNIT_ASSERT_VALUES_EQUAL(authorizeTicket->Get()->TraceContext.RequestId, requestId);
    }
}

} // namespace NKafka::NTests
