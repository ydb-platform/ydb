#include <library/cpp/actors/core/actorsystem.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/log.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/library/services/services.pb.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/http/io/stream.h>
#include <library/cpp/openssl/init/init.h>
#include <library/cpp/openssl/io/stream.h>
#include <util/network/sock.h>
#include <util/system/hostname.h>

using namespace NActors;

namespace NKikimr {

class TNodeIdentifier : public TActorBootstrapped<TNodeIdentifier> {
    using TThis = TNodeIdentifier;
    using TBase = TActorBootstrapped<TNodeIdentifier>;

    struct TEvPrivate {
        enum EEv {
            EvUpdateNodeInfo = EventSpaceBegin(TEvents::ES_PRIVATE),
            EvEnd
        };

        static_assert(EvEnd < EventSpaceEnd(TEvents::ES_PRIVATE), "expected EvEnd < EventSpaceEnd");

        struct TEvUpdateNodeInfo : TEventLocal<TEvUpdateNodeInfo, EvUpdateNodeInfo> {};
    };

    static const NJson::TJsonReaderConfig& GetJsonConfig() {
        static NJson::TJsonReaderConfig jsonConfig;
        return jsonConfig;
    }

    const TString& GetWallEServiceHost() const {
        static const TString host("api.wall-e.yandex-team.ru");
        return host;
    }

    const TNetworkAddress& GetWallENetworkAddress() const {
        static const TNetworkAddress address(GetWallEServiceHost(), 443);
        return address;
    }

    const TString& GetConductorServiceHost() const {
        static const TString host("c.yandex-team.ru");
        return host;
    }

    const TNetworkAddress& GetConductorNetworkAddress() const {
        static const TNetworkAddress address(GetConductorServiceHost(), 443);
        return address;
    }

    TDuration GetTimeout() const {
        return TDuration::Seconds(10);
    }

    void Handle(TEvPrivate::TEvUpdateNodeInfo::TPtr&, const TActorContext& ctx) {
        NKikimrWhiteboard::TSystemStateInfo systemStateInfo;
        TString hostname = FQDNHostName();
        systemStateInfo.SetHost(hostname);

        try {
            TSocket s(GetWallENetworkAddress(), GetTimeout());
            s.SetSocketTimeout(GetTimeout().Seconds());
            TSocketOutput so(s);
            TSocketInput si(s);
            TOpenSslClientIO ssl(&si, &so);
            THttpOutput output(&ssl);
            output << "GET /v1/hosts/" << hostname << "?fields=location HTTP/1.1\r\n"
                   << "Host: " << GetWallEServiceHost() << "\r\n"
                   << "\r\n";
            output.Finish();
            THttpInput input(&ssl);
            unsigned status = ParseHttpRetCode(input.FirstLine());
            if (status == 200) {
                NJson::TJsonValue response;
                bool success = NJson::ReadJsonTree(input.ReadAll(), &GetJsonConfig(), &response);
                if (success) {
                    NJson::TJsonValue* jsonLocation;
                    if (response.GetValuePointer("location", &jsonLocation)) {
                        NJson::TJsonValue* jsonValue;
                        if (jsonLocation->GetValuePointer("short_datacenter_name", &jsonValue)) {
                            if (jsonValue->GetType() == NJson::EJsonValueType::JSON_STRING) {
                                systemStateInfo.SetDataCenter(jsonValue->GetStringRobust());
                            }
                        }
                        if (jsonLocation->GetValuePointer("queue", &jsonValue)) {
                            systemStateInfo.SetDataCenterDescription(jsonValue->GetStringRobust());
                        }
                        if (jsonLocation->GetValuePointer("rack", &jsonValue)) {
                            systemStateInfo.SetRack(jsonValue->GetStringRobust());
                        }
                    }

                }
            }
        }
        catch (const std::exception&) {
        }

        try {
            TSocket s(GetConductorNetworkAddress(), GetTimeout());
            s.SetSocketTimeout(GetTimeout().Seconds());
            TSocketOutput so(s);
            TSocketInput si(s);
            TOpenSslClientIO ssl(&si, &so);
            THttpOutput output(&ssl);

            output << "GET /api/hosts/" << hostname << "?format=json HTTP/1.1\r\n"
                   << "Host: " << GetConductorServiceHost() << "\r\n"
                   << "\r\n";
            output.Finish();
            THttpInput input(&ssl);
            unsigned status = ParseHttpRetCode(input.FirstLine());
            if (status == 200) {
                NJson::TJsonValue response;
                bool success = NJson::ReadJsonTree(input.ReadAll(), &GetJsonConfig(), &response);
                if (success && response.IsArray()) {
                    const NJson::TJsonValue& first = response.GetArray().front();
                    const NJson::TJsonValue* jsonGroup;
                    if (first.GetValuePointer("group", &jsonGroup)) {
                        systemStateInfo.SetClusterName(jsonGroup->GetStringRobust());
                    }
                    const NJson::TJsonValue* jsonRootDatacenter;
                    if (!systemStateInfo.HasDataCenter() && first.GetValuePointer("root_datacenter", &jsonRootDatacenter)) {
                        if (jsonRootDatacenter->GetType() == NJson::EJsonValueType::JSON_STRING) {
                            systemStateInfo.SetDataCenter(jsonRootDatacenter->GetStringRobust());
                        }
                    }
                }
            }
        }
        catch (const std::exception&) {
        }

        TActorId whiteboardServiceId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(ctx.SelfID.NodeId());
        ctx.Send(whiteboardServiceId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(systemStateInfo));
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() { return NKikimrServices::TActivity::NODE_IDENTIFIER; }

    void StateWork(TAutoPtr<IEventHandle>& ev) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvPrivate::TEvUpdateNodeInfo, Handle);
            CFunc(TEvents::TSystem::PoisonPill, Die);
        }
    }

    void Bootstrap(const TActorContext& ctx) {
        ctx.Send(ctx.SelfID, new TEvPrivate::TEvUpdateNodeInfo());
        Become(&TThis::StateWork);
    }
};

IActor* CreateNodeIdentifier() {
    InitOpenSSL();
    return new TNodeIdentifier();
}

}
