#pragma once

#include "flat_bio_eggs.h"
#include "flat_sausage_packet.h"
#include "flat_sausage_fetch.h"
#include <ydb/core/protos/base.pb.h>
#include <ydb/core/base/events.h>
#include <ydb/library/actors/core/event_local.h>

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

        TEvData(TAutoPtr<NPageCollection::TFetch> fetch, EStatus status)
            : Status(status)
            , Fetch(fetch)
        {

        }

        void Describe(IOutputStream &out) const
        {
            out
                << "Blocks{" << Blocks.size() << " pages"
                << " " << Fetch->PageCollection->Label()
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
        TAutoPtr<NPageCollection::TFetch> Fetch;
        TVector<NPageCollection::TLoadedPage> Blocks;
    };

}
}
}
