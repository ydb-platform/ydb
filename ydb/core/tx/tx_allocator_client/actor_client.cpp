#include "actor_client.h"
#include "client.h"

#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/base/appdata.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>

#include <util/generic/deque.h>

namespace NKikimr {

class TTxAllocatorClientActor: public TActorBootstrapped<TTxAllocatorClientActor> {
    struct TDelayedRequest {
        TActorId Sender;
        ui64 Cookie;
        ui64 Count;
    };

    static NTabletPipe::TClientConfig InitPipeClientConfig() {
        NTabletPipe::TClientConfig config;
        config.RetryPolicy = {
            .RetryLimitCount = 3,
            .MinRetryTime = TDuration::MilliSeconds(100),
            .MaxRetryTime = TDuration::Seconds(1),
            .BackoffMultiplier = 5
        };
        return config;
    }

    static const NTabletPipe::TClientConfig& GetPipeClientConfig() {
        static const NTabletPipe::TClientConfig config = InitPipeClientConfig();
        return config;
    }

    void Handle(TEvTxAllocatorClient::TEvAllocate::TPtr& ev, const TActorContext& ctx) {
        auto count = ev->Get()->Count;
        if (0 == count) {
            Send(ev->Sender, new TEvTxAllocatorClient::TEvAllocateResult(TVector<ui64>{}), 0, ev->Cookie);
            return;
        }

        TVector<ui64> txIds = TxAllocatorClient.AllocateTxIds(count, ctx);

        if (txIds) {
            Send(ev->Sender, new TEvTxAllocatorClient::TEvAllocateResult(std::move(txIds)), 0, ev->Cookie);
        } else {
            DelayedRequests.push_back({ev->Sender, ev->Cookie, ev->Get()->Count});
        }
    }

    void Handle(TEvTxAllocator::TEvAllocateResult::TPtr& ev, const TActorContext& ctx) {
        TxAllocatorClient.OnAllocateResult(ev, ctx);

        while (!DelayedRequests.empty()) {
            const TDelayedRequest& request = DelayedRequests.front();

            TVector<ui64> txIds = TxAllocatorClient.AllocateTxIds(request.Count, ctx);

            if (!txIds) {
                break;
            }

            Send(request.Sender, new TEvTxAllocatorClient::TEvAllocateResult(std::move(txIds)), 0, request.Cookie);
            DelayedRequests.pop_front();
        }
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        if (!PipeClientCache->OnConnect(ev)) {
            TxAllocatorClient.SendRequest(ev->Get()->TabletId, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        PipeClientCache->OnDisconnect(ev);
        TxAllocatorClient.SendRequest(ev->Get()->TabletId, ctx);
    }

    void Die(const TActorContext& ctx) override {
        PipeClientCache->Detach(ctx);
        PipeClientCache.Destroy();

        TActor::Die(ctx);
    }

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_ALLOCATOR_CLIENT_ACTOR;
    }

    explicit TTxAllocatorClientActor(TVector<ui64> txAllocators)
        : PipeClientCache(NTabletPipe::CreateUnboundedClientCache(GetPipeClientConfig()))
        , TxAllocatorClient(NKikimrServices::TX_ALLOCATOR_CLIENT, PipeClientCache.Get(), std::move(txAllocators))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        TxAllocatorClient.Bootstrap(ctx);
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxAllocatorClient::TEvAllocate, Handle);

            HFunc(TEvTxAllocator::TEvAllocateResult, Handle);

            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);

            CFunc(TEvents::TEvPoisonPill::EventType, Die);
        }
    }

private:
    THolder<NTabletPipe::IClientCache> PipeClientCache;
    TTxAllocatorClient TxAllocatorClient;

    TDeque<TDelayedRequest> DelayedRequests;

}; // TTxAllocatorClientActor

IActor* CreateTxAllocatorClient(TVector<ui64> txAllocators) {
    return new TTxAllocatorClientActor(std::move(txAllocators));
}

namespace {

TVector<ui64> CollectTxAllocators(const TAppData* appData) {
    TVector<ui64> allocators;
    if (const auto& domain = appData->DomainsInfo->Domain) {
        for (auto tabletId : domain->TxAllocators) {
            allocators.push_back(tabletId);
        }
    }
    return allocators;
}

}

IActor* CreateTxAllocatorClient(const TAppData* appData) {
    return CreateTxAllocatorClient(CollectTxAllocators(appData));
}

} // NKikimr
