#pragma once

#include "flat_sausage_gut.h"

#include <ydb/library/actors/util/shared_data.h>

namespace NKikimr {
namespace NPageCollection {

    struct TFetch {
        TFetch(ui64 cookie, TIntrusiveConstPtr<IPageCollection> pageCollection, TVector<ui32> pages, NWilson::TTraceId traceId = {})
            : Cookie(cookie)
            , PageCollection(std::move(pageCollection))
            , Pages(std::move(pages))
            , TraceId(std::move(traceId))
        {

        }

        void Describe(IOutputStream &out) const noexcept
        {
            out
                << "Fetch{" << Pages.size() << " pages"
                << " " << PageCollection->Label() << "}";
        }

        const ui64 Cookie = Max<ui64>();

        TIntrusiveConstPtr<IPageCollection> PageCollection;
        TVector<ui32> Pages;
        NWilson::TTraceId TraceId;
    };

    struct TLoadedPage {
        TLoadedPage() = default;

        TLoadedPage(ui32 page, TSharedData data)
            : PageId(page)
            , Data(std::move(data))
        {

        }

        explicit operator bool() const noexcept
        {
            return Data && PageId != Max<ui32>();
        }

        ui32 PageId = Max<ui32>();
        TSharedData Data;
    };

}
}
