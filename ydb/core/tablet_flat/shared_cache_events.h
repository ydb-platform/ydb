#pragma once

#include "defs.h"
#include "flat_bio_events.h"
#include "shared_handle.h"
#include <ydb/core/protos/shared_cache.pb.h>

#include <util/generic/map.h>
#include <util/generic/set.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>

namespace NKikimr {
namespace NSharedCache {

    using EPriority = NTabletFlatExecutor::NBlockIO::EPriority;

    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR),

        EvTouch = EvBegin + 512,
        EvUnregister,
        EvInvalidate,
        EvAttach,
        EvRequest,
        EvResult,
        EvUpdated,
        EvReplacementPolicySwitch,

        EvEnd

        /* +1024 range is reserved for scan events */
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_FLAT_EXECUTOR), "");

    struct TEvUnregister : public TEventLocal<TEvUnregister, EvUnregister> {
    };

    struct TEvInvalidate : public TEventLocal<TEvInvalidate, EvInvalidate> {
        const TLogoBlobID PageCollectionId;

        TEvInvalidate(const TLogoBlobID &pageCollectionId)
            : PageCollectionId(pageCollectionId)
        {}
    };

    struct TEvTouch : public TEventLocal<TEvTouch, EvTouch> {
        THashMap<TLogoBlobID, THashMap<ui32, TSharedData>> Touched;

        TEvTouch(THashMap<TLogoBlobID, THashMap<ui32, TSharedData>> &&touched)
            : Touched(std::move(touched))
        {}
    };

    struct TEvAttach : public TEventLocal<TEvAttach, EvAttach> {
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TActorId Owner;

        TEvAttach(TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TActorId owner)
            : PageCollection(std::move(pageCollection))
            , Owner(owner)
        {
            Y_ABORT_UNLESS(Owner, "Cannot send request with empty owner");
        }
    };

    struct TEvRequest : public TEventLocal<TEvRequest, EvRequest> {
        const EPriority Priority;
        TAutoPtr<NPageCollection::TFetch> Fetch;
        TActorId Owner;

        TEvRequest(EPriority priority, TAutoPtr<NPageCollection::TFetch> fetch, TActorId owner)
            : Priority(priority)
            , Fetch(fetch)
            , Owner(owner)
        {
            Y_ABORT_UNLESS(Owner, "Cannot sent request with empty owner");
        }
    };

    struct TEvResult : public TEventLocal<TEvResult, EvResult> {
        using EStatus = NKikimrProto::EReplyStatus;

        TEvResult(TIntrusiveConstPtr<NPageCollection::IPageCollection> origin, ui64 cookie, EStatus status)
            : Status(status)
            , Cookie(cookie)
            , Origin(origin)
        { }

        void Describe(IOutputStream &out) const
        {
            out
                << "TEvResult{" << Loaded.size() << " pages"
                << " " << Origin->Label()
                << " " << (Status == NKikimrProto::OK ? "ok" : "fail")
                << " " << NKikimrProto::EReplyStatus_Name(Status) << "}";
        }

        ui64 Bytes() const
        {
            return
                std::accumulate(Loaded.begin(), Loaded.end(), ui64(0),
                    [](ui64 bytes, const TLoaded& loaded)
                        { return bytes + TPinnedPageRef(loaded.Page)->size(); });
        }

        struct TLoaded {
            TLoaded(ui32 pageId, TSharedPageRef page)
                : PageId(pageId)
                , Page(std::move(page))
            { }

            ui32 PageId;
            TSharedPageRef Page;
        };

        const EStatus Status;
        const ui64 Cookie;
        const TIntrusiveConstPtr<NPageCollection::IPageCollection> Origin;
        TVector<TLoaded> Loaded;
    };

    struct TEvUpdated : public TEventLocal<TEvUpdated, EvUpdated> {
        struct TActions {
            THashMap<ui32, TSharedPageRef> Accepted;
            THashSet<ui32> Dropped;
        };

        THashMap<TLogoBlobID, TActions> Actions;
    };

    struct TEvReplacementPolicySwitch : public TEventLocal<TEvReplacementPolicySwitch, EvReplacementPolicySwitch> {
        using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

        TReplacementPolicy ReplacementPolicy;

        TEvReplacementPolicySwitch(TReplacementPolicy replacementPolicy)
            : ReplacementPolicy(replacementPolicy)
        {}
    };
}
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
