#pragma once

#include "flat_page_iface.h"
#include "flat_sausage_fetch.h"
#include "flat_fwd_misc.h"

namespace NKikimr {
namespace NTable {
    using EPage = NPage::EPage;

namespace NFwd {

    struct TPage;

    class IPageLoadingQueue {
    public:
        virtual ~IPageLoadingQueue() = default;

        virtual ui64 AddToQueue(ui32 page, EPage type) noexcept = 0;
    };

    class IPageLoadingLogic {
    public:
        struct TResult {
            const TSharedData *Page;
            bool Grow; /* Should give more pages on Forward() */
            bool Need; /* Is vital to client to make progress */
        };

        virtual ~IPageLoadingLogic() = default;

        virtual TResult Handle(IPageLoadingQueue *head, ui32 page, ui64 lower) noexcept = 0;
        virtual void Forward(IPageLoadingQueue *head, ui64 upper) noexcept = 0;
        virtual void Apply(TArrayRef<NPageCollection::TLoadedPage> loaded) noexcept = 0;

        IPageLoadingQueue* Head = nullptr; /* will be set outside of IPageLoadingLogic impl */
        TStat Stat;
    };

}
}
}
