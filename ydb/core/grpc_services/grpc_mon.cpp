#include "grpc_mon.h"

#include <ydb/core/base/events.h>
#include <ydb/library/services/services.pb.h>

#include <library/cpp/cache/cache.h>
#include <library/cpp/monlib/service/pages/templates.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/hfunc.h>

namespace NKikimr {
namespace NGRpcService {

struct TEvGrpcMon {
    enum EEv {
        EvReportPeer = EventSpaceBegin(TKikimrEvents::ES_GRPC_MON),

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_MON), "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_GRPC_MON)");

    struct TEvReportPeer: public TEventLocal<TEvReportPeer, EvReportPeer> {
        explicit TEvReportPeer(const TString& name)
            : Name(name)
        {}

        TEvReportPeer(const TString& name, const TString& buildInfo)
            : Name(name)
            , SdkBuildInfo(buildInfo)
        {}

        const TString Name;
        const TString SdkBuildInfo;
    };
};


class TGrpcMon : public TActor<TGrpcMon> {
public:
    TGrpcMon()
        : TActor(&TGrpcMon::StateNormal)
        , Peers(2000)
    {}

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::GRPC_MON;
    }

private:
    STFUNC(StateNormal) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvGrpcMon::TEvReportPeer, HandlePeer);
            HFunc(NMon::TEvHttpInfo, HandleHttp);
        }
    }

    void HandlePeer(TEvGrpcMon::TEvReportPeer::TPtr &ev, const TActorContext &ctx) {
        Y_UNUSED(ctx);
        const auto& info = *ev->Get();
        auto now = TInstant::Now();
        auto it = Peers.Find(info.Name);
        if (it == Peers.End()) {
            TPeerInfo val;
            val.LastRequest = now;
            val.SdkBuildInfo = info.SdkBuildInfo;
            Peers.Insert(info.Name, val);
        } else {
            it->LastRequest = now;
        }
    }

    void HandleHttp(NMon::TEvHttpInfo::TPtr &ev, const TActorContext &ctx) {
        TStringStream str;
        HTML(str) {
            TABLE_SORTABLE_CLASS("table") {
                TABLEHEAD() {
                    TABLER() {
                        TABLEH() { str << "Address";}
                        TABLEH() { str << "Last Request";}
                        TABLEH() { str << "Sdk BuildInfo";}
                    }
                }
                TABLEBODY() {
                    for (auto p = Peers.Begin(); p != Peers.End(); ++p) {
                        TABLER() {
                            TABLED() { str << p.Key(); }
                            TABLED() { str << p.Value().LastRequest.ToRfc822StringLocal(); }
                            TABLED() { str << p.Value().SdkBuildInfo; }
                        }
                    }
                }
            }
        }

        ctx.Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

private:
    struct TPeerInfo {
        TInstant LastRequest;
        TString SdkBuildInfo;
    };

    TLRUCache<TString, TPeerInfo> Peers;
};

TActorId GrpcMonServiceId() {
    const char x[12] = "GrpcMonSvc!";
    return TActorId(0, TStringBuf(x, 12));
}

IActor* CreateGrpcMonService() {
    return new TGrpcMon;
}

void ReportGrpcReqToMon(NActors::TActorSystem& actorSystem, const TString& fromAddress) {
    actorSystem.Send(GrpcMonServiceId(), new TEvGrpcMon::TEvReportPeer(fromAddress));
}

void ReportGrpcReqToMon(NActors::TActorSystem& actorSystem, const TString& fromAddress, const TString& buildInfo) {
    actorSystem.Send(GrpcMonServiceId(), new TEvGrpcMon::TEvReportPeer(fromAddress, buildInfo));
}

}}
