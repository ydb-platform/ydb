#pragma once

#include "flat_bio_eggs.h"
#include "flat_sausage_fetch.h"
#include "flat_sausage_gut.h"
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_local.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBlockIO {

    using TPageId = NPageCollection::TPageId;

    enum class EEv : ui32 {
        Base_ = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR) + 1088,

        Fetch   = Base_ + 0,
        Data    = Base_ + 1,
        Stat    = Base_ + 8,
    };

    struct TEvFetch : public TEventLocal<TEvFetch, ui32(EEv::Fetch)> {
        TEvFetch(EPriority priority, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, TVector<TPageId> pages, ui64 cookie)
            : Priority(priority)
            , PageCollection(std::move(pageCollection))
            , Pages(std::move(pages))
            , Cookie(cookie)
        {

        }

        const EPriority Priority;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<TPageId> Pages;
        NWilson::TTraceId TraceId;
        const ui64 Cookie;
    };

    struct TEvData: public TEventLocal<TEvData, ui32(EEv::Data)> {
        using EStatus = NKikimrProto::EReplyStatus;

        TEvData(EStatus status, TIntrusiveConstPtr<NPageCollection::IPageCollection> pageCollection, ui64 cookie)
            : Status(status)
            , PageCollection(std::move(pageCollection))
            , Cookie(cookie)
        {

        }

        void Describe(IOutputStream &out) const
        {
            out
                << "Blocks{" << Pages.size() << " pages"
                << " " << PageCollection->Label()
                << " " << (Status == NKikimrProto::OK ? "ok" : "fail")
                << " " << NKikimrProto::EReplyStatus_Name(Status) << "}";
        }

        const EStatus Status;
        TIntrusiveConstPtr<NPageCollection::IPageCollection> PageCollection;
        TVector<NPageCollection::TLoadedPage> Pages;
        const ui64 Cookie;
    };

}
}
}
