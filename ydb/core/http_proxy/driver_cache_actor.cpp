#include "driver_cache_actor.h"
#include "events.h"
#include "http_req.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/actors/http/http_proxy.h>
#include <library/cpp/cache/cache.h>

#include <util/stream/file.h>
#include <util/string/builder.h>
#include <util/string/vector.h>
#include <util/system/hostname.h>

namespace NKikimr::NHttpProxy {

    using namespace NActors;


    class TStaticDiscoveryActor : public NActors::TActorBootstrapped<TStaticDiscoveryActor> {
        using TBase = NActors::TActorBootstrapped<TStaticDiscoveryActor>;
    public:
        explicit TStaticDiscoveryActor(ui16 port)
        {
            Database.Reset(new TDatabase(TStringBuilder()  << FQDNHostName() << ":" << port, "database_id", "", "cloud_id", "folder_id"));
        }

        void Bootstrap(const TActorContext& ctx) {
            TBase::Become(&TStaticDiscoveryActor::StateWork);
            Y_UNUSED(ctx);
        }

    private:
        STFUNC(StateWork) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest, Handle);
            }
        }

        THolder<TDatabase> Database;

        TVector<std::pair<TActorId, TString>> Waiters;

        void Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest::TPtr& ev, const TActorContext& ctx);

        TStringBuilder LogPrefix() const {
            return TStringBuilder();
        }

        void ProcessWaiters(const TActorContext& ctx);
    };


    void TStaticDiscoveryActor::Handle(TEvServerlessProxy::TEvDiscoverDatabaseEndpointRequest::TPtr& ev, const TActorContext& ctx) {
        Waiters.push_back(std::make_pair(ev->Sender, ev->Get()->DatabasePath));
        if (!Database) {
            return;
        }
        ProcessWaiters(ctx);
    }

    void TStaticDiscoveryActor::ProcessWaiters(const TActorContext& ctx) {
        Y_VERIFY(Database);
        for (auto& waiter : Waiters) {
            auto result = MakeHolder<TEvServerlessProxy::TEvDiscoverDatabaseEndpointResult>();
            auto database = waiter.second;
            result->DatabaseInfo = MakeHolder<TDatabase>(*Database);
            result->DatabaseInfo->Path = database;
            result->Status = NYdb::EStatus::SUCCESS;
            ctx.Send(waiter.first, std::move(result));
        }
        Waiters.clear();
    }

    NActors::IActor* CreateStaticDiscoveryActor(ui16 port) {
        return new TStaticDiscoveryActor{port};
    }

}
