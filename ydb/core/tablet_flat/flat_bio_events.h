#pragma once

#include "flat_bio_eggs.h"
#include "flat_sausage_packet.h"
#include "flat_sausage_fetch.h"
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/base/events.h>
#include <library/cpp/actors/core/event_local.h>

namespace NKikimr {
namespace NTabletFlatExecutor {
namespace NBlockIO {

    enum class EEv : ui32 {
        Base_ = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR) + 1088,

        Fetch   = Base_ + 0,
        Data    = Base_ + 1,
        Stat    = Base_ + 8,
    };

    struct TEvFetch : public TEventLocal<TEvFetch, ui32(EEv::Fetch)> {
        TEvFetch(EPriority priority, TAutoPtr<NPageCollection::TFetch> fetch)
            : Priority(priority)
            , Fetch(fetch)
        {

        }

        const EPriority Priority = EPriority::None;
        TAutoPtr<NPageCollection::TFetch> Fetch;
    };

    struct TEvData: public TEventLocal<TEvData, ui32(EEv::Data)> {
        using EStatus = NKikimrProto::EReplyStatus;

        TEvData(TIntrusiveConstPtr<NPageCollection::IPageCollection> origin, ui64 cookie, EStatus status)
            : Status(status)
            , Cookie(cookie)
            , Origin(origin)
        {

        }

        void Describe(IOutputStream &out) const
        {
            out
                << "Blocks{" << Blocks.size() << " pages"
                << " " << Origin->Label()
                << " " << (Status == NKikimrProto::OK ? "ok" : "fail")
                << " " << NKikimrProto::EReplyStatus_Name(Status) << "}";
        }

        ui64 Bytes() const
        {
            return
                std::accumulate(Blocks.begin(), Blocks.end(), ui64(0),
                    [](ui64 bytes, const NPageCollection::TLoadedPage& block)
                        { return bytes + block.Data.size(); });
        }

        const EStatus Status;
        const ui64 Cookie = Max<ui64>();
        TIntrusiveConstPtr<NPageCollection::IPageCollection> Origin;
        TVector<NPageCollection::TLoadedPage> Blocks;
    };

}
}
}
