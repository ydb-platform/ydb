#pragma once

#include "defs.h"
#include "flat_bio_events.h"
#include "shared_handle.h"
#include "shared_page.h"
#include <ydb/core/protos/shared_cache.pb.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr::NSharedCache {
    using EPriority = NTabletFlatExecutor::NBlockIO::EPriority;
    using TPageId = NTable::NPage::TPageId;

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

        EvTouch = EvBegin + 512,
        EvUnregister,
        EvDetach,
        EvAttach,
        EvSaveCompactedPages,
        EvRequest,
        EvResult,
        EvUpdated,

        EvEnd

        /* +1024 range is reserved for scan events */
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_EXECUTOR), "");

    struct TEvUnregister : public TEventLocal<TEvUnregister, EvUnregister> {
    };

    struct TEvDetach : public TEventLocal<TEvDetach, EvDetach> {
        const TLogoBlobID PageCollectionId;

        TEvDetach(const TLogoBlobID &pageCollectionId)
            : PageCollectionId(pageCollectionId)
        {}
    };

    struct TEvTouch : public TEventLocal<TEvTouch, EvTouch> {
        THashMap<TLogoBlobID, THashSet<TPageId>> Touched;

        TEvTouch(THashMap<TLogoBlobID, THashSet<TPageId>> &&touched)
            : Touched(std::move(touched))
        {}
    };

    struct TEvAttach : public TEventLocal<TEvAttach, EvAttach> {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        ECacheMode CacheMode;

        TEvAttach(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, ECacheMode cacheMode)
            : PageCollection(std::move(pageCollection))
            , CacheMode(cacheMode)
        {
        }
    };

    // Note: compacted pages do not have an owner yet
    // at first they should be accepted by an executor
    // and it will send TEvAttach itself when it have happened
    struct TEvSaveCompactedPages : public TEventLocal<TEvSaveCompactedPages, EvSaveCompactedPages> {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TIntrusivePtr<TPage>> Pages;

        TEvSaveCompactedPages(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection)
            : PageCollection(std::move(pageCollection))
        {
        }
    };

    struct TEvRequest : public TEventLocal<TEvRequest, EvRequest> {
        TEvRequest(EPriority priority, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TVector<TPageId> pages, ui64 cookie = 0)
            : Priority(priority)
            , PageCollection(std::move(pageCollection))
            , Pages(std::move(pages))
            , Cookie(cookie)
        { }

        const EPriority Priority;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TPageId> Pages;
        TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
        NWilson::TTraceId TraceId;
        const ui64 Cookie;
    };

    struct TEvResult : public TEventLocal<TEvResult, EvResult> {
        using EStatus = NKikimrProto::EReplyStatus;

        TEvResult(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, EStatus status, ui64 cookie)
            : Status(status)
            , PageCollection(std::move(pageCollection))
            , Cookie(cookie)
        { }

        void Describe(IOutputStream &out) const
        {
            out
                << "TEvResult{" << Pages.size() << " pages"
                << " " << PageCollection->Label()
                << " " << (Status == NKikimrProto::OK ? "ok" : "fail")
                << " " << NKikimrProto::EReplyStatus_Name(Status) << "}";
        }

        ui64 Bytes() const
        {
            return
                std::accumulate(Pages.begin(), Pages.end(), ui64(0),
                    [](ui64 bytes, const TLoaded& loaded)
                        { return bytes + TPinnedPageRef(loaded.Page)->size(); });
        }

        struct TLoaded {
            TLoaded(TPageId pageId, TSharedPageRef page)
                : PageId(pageId)
                , Page(std::move(page))
            { }

            TPageId PageId;
            TSharedPageRef Page;
        };

        const EStatus Status;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TLoaded> Pages;
        TIntrusivePtr<NPageCollection::TPagesWaitPad> WaitPad;
        const ui64 Cookie;
    };

    struct TEvUpdated : public TEventLocal<TEvUpdated, EvUpdated> {
        THashMap<TLogoBlobID, THashSet<TPageId>> DroppedPages;
    };
}

template<> inline
void Out<NKikimr::NTabletFlatExecutor::NBlockIO::EPriority>(
        IOutputStream& o,
        NKikimr::NTabletFlatExecutor::NBlockIO::EPriority value)
{
    switch (value) {
    case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Fast:
        o << "Online";
        break;
    case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Bkgr:
        o << "AsyncLoad";
        break;
    case NKikimr::NTabletFlatExecutor::NBlockIO::EPriority::Bulk:
        o << "Scan";
        break;
    default:
        o << static_cast<ui32>(value);
        break;
    }
}
