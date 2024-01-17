#include "dsproxy_nodemonactor.h"
#include "dsproxy_nodemon.h"

#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/mon.h>

#include <library/cpp/monlib/service/pages/templates.h>

namespace NKikimr {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// TDsProxyNodeMonActor
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

class TDsProxyNodeMonActor : public TActorBootstrapped<TDsProxyNodeMonActor> {

    TIntrusivePtr<TDsProxyNodeMon> Mon;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::DS_PROXY_NODE_MON_ACTOR;
    }

    TDsProxyNodeMonActor(TIntrusivePtr<TDsProxyNodeMon> mon)
        : Mon(std::move(mon))
    {}

    ~TDsProxyNodeMonActor() {
    }


    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor handlers
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Bootstrap state
    void Bootstrap() {
        NActors::TMon* mon = AppData()->Mon;
        if (mon) {
            NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
            mon->RegisterActorPage(actorsMonPage, "dsproxynode", "DsProxyNode", false, TlsActivationContext->ExecutorThread.ActorSystem,
                SelfId());
        }

        Become(&TThis::StateOnline);
        HandleWakeup();
    }

    void HandleWakeup() {
        Schedule(TDuration::Seconds(1), new TEvents::TEvWakeup);
        Mon->PutResponseTime.Update();

        Mon->PutTabletLogResponseTime.Update();
        Mon->PutTabletLogResponseTime256.Update();
        Mon->PutTabletLogResponseTime512.Update();

        Mon->PutAsyncBlobResponseTime.Update();
        Mon->PutUserDataResponseTime.Update();

        Mon->GetResponseTime.Update();

        Mon->BlockResponseTime.Update();
        Mon->DiscoverResponseTime.Update();
        Mon->IndexRestoreGetResponseTime.Update();
        Mon->RangeResponseTime.Update();
        Mon->PatchResponseTime.Update();
    }

    void Handle(NMon::TEvHttpInfo::TPtr &ev) {
        TStringStream str;

        HTML(str) {
            DIV_CLASS("panel panel-info") {
                DIV_CLASS("panel-heading") {
                    str << "Distributed Storage Proxy Node Monitoring";
                }
            }
        }

        Send(ev->Sender, new NMon::TEvHttpInfoRes(str.Str()));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // Actor state functions
    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    STATEFN(StateOnline) {
        switch (ev->GetTypeRewrite()) {
            cFunc(NActors::TEvents::TSystem::Poison, PassAway);
            hFunc(NMon::TEvHttpInfo, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
        default:
            break;
        }
    }
};

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Blob Storage Group Proxy Mon Creation
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
IActor* CreateDsProxyNodeMon(TIntrusivePtr<TDsProxyNodeMon> mon) {
    return new TDsProxyNodeMonActor(mon);
}

} // NKikimr

