#pragma once

#include "flat_sausage_gut.h"

#include <util/generic/xrange.h>
#include <ydb/library/actors/util/shared_data.h>

namespace NKikimr {
namespace NPageCollection {

    struct TPagesWaitPad : public TThrRefBase {
        ui64 PendingRequests = 0;
    };

    struct TFetch {
        TFetch(ui64 cookie, TIntrusiveConstPtr<IPageCollection> pageCollection, TVector<TPageId> pages)
            : Cookie(cookie)
            , PageCollection(std::move(pageCollection))
            , Pages(std::move(pages))
        {
        }

        TString DebugString(bool detailed = false) const
        {
            TStringBuilder str;
            str << "PageCollection: " << PageCollection->Label();
            if (detailed) {
                str << " Pages: [";
                for (const auto& pageId : Pages) {
                    str << " " << pageId;
                }
                str << " ]";
            } else {
                str << " Pages: " << Pages.size();
            }
            if (Cookie != Max<ui64>()) str << " Cookie: " << Cookie;
            return str;
        }

        ui64 Cookie = Max<ui64>();
        TIntrusiveConstPtr<IPageCollection> PageCollection;
        TVector<TPageId> Pages;
        TIntrusivePtr<TPagesWaitPad> WaitPad;
        NWilson::TTraceId TraceId;
    };

    struct TLoadedPage {
        TLoadedPage() = default;

        TLoadedPage(TPageId page, TSharedData data)
            : PageId(page)
            , Data(std::move(data))
        {

        }

        explicit operator bool() const noexcept
        {
            return Data && PageId != Max<TPageId>();
        }

        TPageId PageId = Max<TPageId>();
        TSharedData Data;
    };

}
}
