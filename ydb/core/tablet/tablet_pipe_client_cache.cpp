#include "tablet_pipe_client_cache.h"
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/core/util/cache.h>
#include <util/generic/map.h>
#include <util/system/mutex.h>

namespace NKikimr {

namespace NTabletPipe {

    IClientFactory::~IClientFactory() {}

    struct TClientCacheEntry {
        enum {
            Opened = 1 << 0,
            ShutdownRequested = 1 << 1,
        };

        TActorId Client;
        ui32 Flags;

        TClientCacheEntry()
            : Flags(0)
        {
        }

        TClientCacheEntry(const TActorId& client, ui32 flags)
            : Client(client)
            , Flags(flags)
        {
        }
    };

    typedef NCache::ICache<ui64, TClientCacheEntry> IClientCacheContainer;

    class TClientCache : public IClientCache {
    public:
        TClientCache(const TClientConfig& config, TAutoPtr<IClientCacheContainer> container, TAutoPtr<IClientCacheContainer> poolContainer, IClientFactory::TPtr pipeFactory)
            : PipeClientConfig(config)
            , Container(container)
            , PoolContainer(poolContainer)
            , ActorSystem(nullptr)
            , PipeFactory(std::move(pipeFactory))
        {
            Container->SetEvictionCallback([&](const ui64& key, TClientCacheEntry& value, ui64 size) {
                return EvictionCallback(key, value, size);
            });

            if (PoolContainer) {
                PoolContainer->SetEvictionCallback([&](const ui64& key, TClientCacheEntry& value, ui64 size) {
                    return EvictionCallback(key, value, size);
                });
            }
        }

        ~TClientCache() {
            Container->SetEvictionCallback(&IClientCacheContainer::DefaultEvictionCallback);
            if (PoolContainer) {
                PoolContainer->SetEvictionCallback(&IClientCacheContainer::DefaultEvictionCallback);
            }
        }

        void EvictionCallback(const ui64& key, TClientCacheEntry& value, ui64 size) {
            Y_UNUSED(key);
            Y_UNUSED(size);
            if (!value.Client)
                return;

            ActorSystem->Send(value.Client, new TEvents::TEvPoisonPill);
        }

        TActorId Prepare(const TActorContext& ctx, ui64 tabletId) override {
            TClientCacheEntry* currentClient;
            if (Container->Find(tabletId, currentClient)) {
                currentClient->Flags &= ~TClientCacheEntry::ShutdownRequested;
                return currentClient->Client;
            }

            if (PoolContainer) {
                if (PoolContainer->Find(tabletId, currentClient)) {
                    TClientCacheEntry* insertedClient;
                    Container->Insert(tabletId, *currentClient, insertedClient);
                    currentClient->Client = TActorId();
                    insertedClient->Flags &= ~TClientCacheEntry::ShutdownRequested;
                    return insertedClient->Client;
                }
            }

            ActorSystem = ctx.ExecutorThread.ActorSystem;
            TActorId clientId;
            if (PipeFactory) {
                clientId = PipeFactory->CreateClient(ctx, tabletId, PipeClientConfig);
            } else {
                IActor* client = CreateClient(ctx.SelfID, tabletId, PipeClientConfig);
                clientId = ctx.ExecutorThread.RegisterActor(client);
            }
            Container->Insert(tabletId, TClientCacheEntry(clientId, 0), currentClient);
            return clientId;
        }

        TActorId Send(const TActorContext& ctx, ui64 tabletId, IEventBase* payload, ui64 cookie) override {
            auto clientId = Prepare(ctx, tabletId);
            SendData(ctx, clientId, payload, cookie);
            return clientId;
        }

        TActorId Send(const TActorContext& ctx, ui64 tabletId, ui32 eventType, const TIntrusivePtr<TEventSerializedData>& eventBuffer, ui64 cookie) override {
            auto clientId = Prepare(ctx, tabletId);
            SendData(ctx, clientId, eventType, eventBuffer, cookie);
            return clientId;
        }

        void Detach(const TActorContext& ctx) override {
            Y_UNUSED(ctx);
            Container->Clear();
            if (PoolContainer) {
                PoolContainer->Clear();
            }
        }

        bool OnConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) override {
            if (ev->Get()->Status != NKikimrProto::OK) {
                Erase(ev->Get()->TabletId, ev->Get()->ClientId);
                return false;
            }

            TClientCacheEntry* currentClient;
            if (Container->Find(ev->Get()->TabletId, currentClient) &&
                currentClient->Client == ev->Get()->ClientId)
            {
                currentClient->Flags |= TClientCacheEntry::Opened;
                if (currentClient->Flags & TClientCacheEntry::ShutdownRequested) {
                    currentClient->Flags &= ~TClientCacheEntry::ShutdownRequested;
                    MoveToPool(ev->Get()->TabletId, *currentClient);
                }
            }

            return true;
        }

        void OnDisconnect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) override {
            Erase(ev->Get()->TabletId, ev->Get()->ClientId);
        }

        void Close(const TActorContext& ctx, ui64 tabletId) override {
            Y_UNUSED(ctx);
            Container->Erase(tabletId);
            if (PoolContainer)
                PoolContainer->Erase(tabletId);
        }

        void ForceClose(const TActorContext& ctx, ui64 tabletId) override {
            TClientCacheEntry* currentClient;
            if (!Container->Find(tabletId, currentClient))
                return;

            TActorId client = currentClient->Client;
            CloseClient(ctx, client);
            Container->Erase(tabletId);
        }

        void Shutdown(const TActorContext& ctx, ui64 tabletId) override {
            TClientCacheEntry* currentClient;
            if (!Container->Find(tabletId, currentClient))
                return;

            TActorId client = currentClient->Client;
            if (!PoolContainer) {
                CloseClient(ctx, client);
                Container->Erase(tabletId);
            } else {
                if (currentClient->Flags & TClientCacheEntry::Opened) {
                    MoveToPool(tabletId, *currentClient);
                } else {
                    currentClient->Flags |= TClientCacheEntry::ShutdownRequested;
                }
            }
        }

        void PopWhileOverflow() override {
            if (PoolContainer)
                PoolContainer->PopWhileOverflow();
        }

    private:
        void MoveToPool(ui64 tabletId, TClientCacheEntry& currentClient) {
            TClientCacheEntry* insertedClient;
            if (!PoolContainer->Insert(tabletId, currentClient, insertedClient)) {
                Y_DEBUG_ABORT_UNLESS(!insertedClient->Client);
                *insertedClient = currentClient;
            }

            // Note: client was moved to pool, make sure it's not closed by
            // the eviction callback
            currentClient.Client = {};

            Container->Erase(tabletId);
        }

        void Erase(ui64 tabletId, const TActorId& clientId) {
            TClientCacheEntry* currentClient;
            if (Container->Find(tabletId, currentClient)) {
                Y_DEBUG_ABORT_UNLESS(!!currentClient->Client);
                if (!clientId || (currentClient->Client == clientId))
                    Container->Erase(tabletId);
            }

            if (PoolContainer && PoolContainer->Find(tabletId, currentClient)) {
                if (!clientId || !currentClient->Client || (currentClient->Client == clientId))
                    PoolContainer->Erase(tabletId);
            }
        }

    private:
        TClientConfig PipeClientConfig;
        TAutoPtr<IClientCacheContainer> Container;
        TAutoPtr<IClientCacheContainer> PoolContainer;

        TActorSystem* ActorSystem;

        IClientFactory::TPtr PipeFactory;
    };

    IClientCache* CreateUnboundedClientCache(const TClientConfig& pipeConfig, IClientFactory::TPtr pipeFactory) {
        TAutoPtr<IClientCacheContainer> container(new NCache::TUnboundedCacheOnMap<ui64, TClientCacheEntry>());
        return new TClientCache(pipeConfig, container, TAutoPtr<IClientCacheContainer>(), std::move(pipeFactory));
    }

    IClientCache* CreateBoundedClientCache(TIntrusivePtr<TBoundedClientCacheConfig> cacheConfig, const TClientConfig& pipeConfig, IClientFactory::TPtr pipeFactory) {
        TAutoPtr<IClientCacheContainer> container(new NCache::TUnboundedCacheOnMap<ui64, TClientCacheEntry>());
        TIntrusivePtr<NCache::T2QCacheConfig> poolContainerConfig(new NCache::T2QCacheConfig);
        TAutoPtr<IClientCacheContainer> poolContainer(new NCache::T2QCache<ui64, TClientCacheEntry>(poolContainerConfig));
        poolContainer->SetOverflowCallback([=](const IClientCacheContainer& cache) {
            return cache.GetUsedSize() >= cacheConfig->ClientPoolLimit;
        });

        return new TClientCache(pipeConfig, container, poolContainer, std::move(pipeFactory));
    }
}

}
