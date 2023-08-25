#pragma once
#include "defs.h"
#include "flat_sausage_flow.h"
#include "flat_bio_events.h"
#include "util_fmt_line.h"
#include <ydb/core/tablet/tablet_metrics.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBlockIO {

    class TBlockIO : public ::NActors::IActorCallback {
        using TEventHandlePtr = TAutoPtr<::NActors::IEventHandle>;
        using ELnLev = NUtil::ELnLev;
        using EStatus = NKikimrProto::EReplyStatus;
        using TPagesToBlobsConverter = NPageCollection::TPagesToBlobsConverter<NPageCollection::IPageCollection>;

        struct TLoaded; /* hack for fwd decl of BS interface units */

    public:
        TBlockIO(TActorId service, ui64 cookie);
        ~TBlockIO();

    private:
        void Registered(TActorSystem*, const TActorId&) override;
        void Inbox(TEventHandlePtr &eh);
        void Bootstrap(EPriority priority, TAutoPtr<NPageCollection::TFetch>) noexcept;
        void Dispatch() noexcept;
        void Handle(ui32 offset, TArrayRef<TLoaded>) noexcept;
        void Terminate(EStatus code) noexcept;

    private:
        const TActorId Service;
        const ui64 Cookie = Max<ui64>();
        TAutoPtr<NUtil::ILogger> Logger;

        /*_ immutable request settings  */

        TActorId Owner;
        EPriority Priority = EPriority::None;
        TAutoPtr<NPageCollection::TFetch> Origin;

        /*_ request operational state   */

        struct TLoadState {
            size_t Offset = 0;
            TSharedData Data;

            explicit TLoadState(size_t size)
                : Data(TSharedData::Uninitialized(size))
            { }
        };

        TVector<TLoadState> BlockStates;
        TAutoPtr<TPagesToBlobsConverter> PagesToBlobsConverter;
        ui64 Pending = 0;
        ui64 TotalOps = 0;
        NMetrics::TTabletThroughputRawValue GroupBytes;
        NMetrics::TTabletIopsRawValue GroupOps;
    };

    template<typename ... TArgs>
    inline void Start(NActors::IActorOps *ops, TActorId service,
                        ui64 cookie, TArgs&& ... args) noexcept
    {
        auto self = ops->Register(new TBlockIO(service, cookie));

        ops->Send(self, new TEvFetch(std::forward<TArgs>(args)...));
    }

}
}
}
