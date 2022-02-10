#pragma once
#include <ydb/core/base/tablet_pipe.h>

namespace NKikimr {

namespace NCache {
    struct TCacheStatistics;
}

namespace NTabletPipe {

    class IClientFactory : public TThrRefBase {
    public:
        using TPtr = TIntrusivePtr<IClientFactory>;

        virtual ~IClientFactory();

        virtual TActorId CreateClient(const TActorContext& ctx, ui64 tabletId, const TClientConfig& pipeConfig) = 0;
    };

    class IClientCache {
    public:
        virtual ~IClientCache() {}
        virtual TActorId Prepare(const TActorContext& ctx, ui64 tabletId) = 0;
        virtual TActorId Send(const TActorContext& ctx, ui64 tabletId, ui32 eventType, const TIntrusivePtr<TEventSerializedData>& eventBuffer, ui64 cookie = 0) = 0;
        virtual TActorId Send(const TActorContext& ctx, ui64 tabletId, IEventBase* payload, ui64 cookie = 0) = 0;
        virtual void Detach(const TActorContext& ctx) = 0;
        // returns true if connect was successfull
        virtual bool OnConnect(TEvTabletPipe::TEvClientConnected::TPtr& ev) = 0;
        virtual void OnDisconnect(TEvTabletPipe::TEvClientDestroyed::TPtr& ev) = 0;
        virtual void Close(const TActorContext& ctx, ui64 tabletId) = 0;
        virtual void ForceClose(const TActorContext& ctx, ui64 tabletId) = 0;
        virtual void Shutdown(const TActorContext& ctx, ui64 tabletId) = 0;
        virtual void PopWhileOverflow() = 0;
    };

    IClientCache* CreateUnboundedClientCache(const TClientConfig& pipeConfig = TClientConfig(), IClientFactory::TPtr pipeFactory = nullptr);

    struct TBoundedClientCacheConfig : public TThrRefBase {
        ui64 ClientPoolLimit;

        TBoundedClientCacheConfig()
            : ClientPoolLimit(100)
        {}
    };

    IClientCache* CreateBoundedClientCache(TIntrusivePtr<TBoundedClientCacheConfig> cacheConfig, const TClientConfig& pipeConfig = TClientConfig(), IClientFactory::TPtr pipeFactory = nullptr);
}

}
