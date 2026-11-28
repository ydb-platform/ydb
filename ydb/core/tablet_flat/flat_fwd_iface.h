#pragma once

#include "flat_part_iface.h"
#include "flat_transaction_pages.h"
#include "flat_sausage_fetch.h"
#include "flat_fwd_misc.h"
#include "shared_handle.h"

namespace NKikimr {
namespace NTable {
    struct TSubset;
    struct TPartView;
    using EPage = NPage::EPage;
    using TPageId = NPage::TPageId;

namespace NFwd {

    using TPageOffset = NPage::TPageOffset;
    using TPageLocation = NPage::TPageLocation;

    struct TPage;
    struct TConf;
    struct TSeen;

    struct TFetch : TMoveOnly {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<NTable::NPage::TPageLocation> Pages;
        ui64 Cookie;

        explicit operator bool() const {
            return bool(Pages);
        }
    };

    struct IEnv : public IPages {
        virtual const NTabletFlatExecutor::IPageCacheProtocol& Protocol() const noexcept = 0;
        virtual THolder<NActors::IEventBase> MakeRequest(
            NTabletFlatExecutor::NBlockIO::EPriority priority, TFetch fetch) const = 0;
        virtual THolder<IPartLoaderEnv> MakeLoaderEnvironment() const = 0;

        // One working set covers the existing subset, cold loaders and newly discovered collections
        virtual TIntrusivePtr<NTabletFlatExecutor::IPageMetadataWorkingSet> MetadataWorkingSet() const {
            return {};
        }

        // Include physical and Pseudo identities in the backend's metadata group
        virtual void AddCold(const TPartView& partView) = 0;
        virtual bool MayProgress() const = 0;
        virtual void Save(NActors::IEventBase& event) = 0;
        virtual IPages* Reset() = 0;
        virtual TStat Stats() const = 0;
        virtual TFetch GetFetch() = 0;
        virtual TAutoPtr<TSeen> GrabTraces() = 0;
    };

    THolder<IEnv> MakeEnv(const TConf& conf, const TSubset& subset);

    class IPageLoadingQueue {
    public:
        virtual ~IPageLoadingQueue() = default;

        virtual ui64 AddToQueue(TPageOffset offset, EPage type, ui64 size, ui32 crc32) = 0;
    };

    class IPageLoadingLogic {
    public:
        struct TResult {
            const TSharedData *Page;
            bool Grow; /* Should give more pages on Forward() */
            bool Need; /* Is vital to client to make progress */
        };

        virtual ~IPageLoadingLogic() = default;

        virtual TResult Get(IPageLoadingQueue *head, TPageOffset offset, EPage type, ui64 lower) = 0;
        virtual void Forward(IPageLoadingQueue *head, ui64 upper) = 0;
        virtual void Fill(NPageCollection::TLoadedPage& page, NSharedCache::TSharedPageRef sharedPageRef, EPage type) = 0;

        IPageLoadingQueue* Head = nullptr; /* will be set outside of IPageLoadingLogic impl */
        TStat Stat;
    };

}
}
}
