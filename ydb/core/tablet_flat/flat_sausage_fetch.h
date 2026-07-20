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

        TLoadedPage(TPageLocation location, TSharedData data)
            : Location(location)
            , Data(std::move(data))
        {

        }

        explicit operator bool() const noexcept
        {
            return Data && bool(Location);
        }

        TPageLocation Location;
        TSharedData Data;
    };

}
}
