#pragma once

#include "flat_sausage_solid.h"

#include <util/generic/xrange.h>
#include <ydb/library/actors/util/shared_data.h>

namespace NKikimr {
namespace NPageCollection {

    struct TPagesWaitPad : public TThrRefBase {
        ui64 PendingRequests = 0;
    };

    struct TLoadedPage {
        TLoadedPage() = default;

        TLoadedPage(NTable::NPage::TPageLocation location, TSharedData data)
            : Location(location)
            , Data(std::move(data))
        {

        }

        TLoadedPage(TPageId page, TSharedData data) = delete;

        explicit operator bool() const noexcept
        {
            return Data && bool(Location);
        }

        NTable::NPage::TPageLocation Location;
        TPageId PageId = ::Max<TPageId>();
        TSharedData Data;
    };

}
}
