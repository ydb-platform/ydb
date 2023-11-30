#include "health.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actor.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/mon/mon.h>
#include <library/cpp/monlib/service/pages/templates.h>

#include <ydb/public/sdk/cpp/client/ydb_discovery/discovery.h>

namespace NFq {
namespace {

using namespace NActors;

class THealthActor : public NActors::TActorBootstrapped<THealthActor> {
    NConfig::THealthConfig Config;
    NYdb::NDiscovery::TDiscoveryClient Client;
    NFq::TYqSharedResources::TPtr YqSharedResources;

public:
    THealthActor(const NConfig::THealthConfig& config, const NFq::TYqSharedResources::TPtr& yqSharedResources, const ::NMonitoring::TDynamicCounterPtr& )
        : Config(config)
        , Client(yqSharedResources->CoreYdbDriver,
            NYdb::TCommonClientSettings()
                .DiscoveryEndpoint("localhost:" + ToString(Config.GetPort()))
                .SslCredentials(NYdb::TSslCredentials(Config.GetSecure()))
                .Database(Config.GetDatabase()))
        , YqSharedResources(yqSharedResources)
    {
    }

    static constexpr char ActorName[] = "YQ_HEALTH";

    void Bootstrap() {
        HEALTH_LOG_D("Starting yandex query health. Actor id: " << SelfId());

        NActors::TMon* mon = NKikimr::AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage* actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "yq_health", "YQ Health", false,
                TlsActivationContext->ExecutorThread.ActorSystem, SelfId());
        }

        Become(&THealthActor::StateFunc);
    }

private:
    STRICT_STFUNC(StateFunc,
        hFunc(NMon::TEvHttpInfo, Handle);
    )

    void Handle(NMon::TEvHttpInfo::TPtr& ev) {
        TStringStream str;
        auto future = Client.ListEndpoints();
        auto value = future.GetValue(TDuration::Minutes(1));
        if (value.IsSuccess() && value.GetIssues().Empty()) {
            str << NMonitoring::HTTPOKTEXT;
            str << "ok /ping ";
            str << value.GetEndpointsInfo().size();
        } else {
            TStringStream body;
            body << "<html><body><h1>503 Actor Unavailable</h1>";
            body << "<p>" << value.IsSuccess() << ": " << value.GetIssues().ToOneLineString() << "</p>";
            body << "</body></html>";
            str << "HTTP/1.1 503 Actor Unavailable\r\n";
            str << "Content-Type: text/html\r\n";
            str << "Content-Length: " << body.Size() << "\r\n";
            str << "\r\n";
            str << body.Str();
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str(), 0, NMon::TEvHttpInfoRes::EContentType::Custom));
    }
};

} // namespace

TActorId HealthActorId() {
    constexpr TStringBuf name = "YQHLTH";
    return NActors::TActorId(0, name);
}

IActor* CreateHealthActor(const NConfig::THealthConfig& config, const NFq::TYqSharedResources::TPtr& yqSharedResources, const ::NMonitoring::TDynamicCounterPtr& counters) {
    return new THealthActor(config, yqSharedResources, counters);
}

}  // namespace NFq
