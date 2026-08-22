#pragma once

#include "shared_handle.h"
#include "shared_cache_s3fifo.h"
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NSharedCache {

using TPageId = NTable::NPage::TPageId;
using EPage = NTable::NPage::EPage;
using ECacheMode = NTable::NPage::ECacheMode;

struct TCollection;

enum EPageState {
    PageStateNo,
    PageStateLoaded,
    PageStateRequested,
    PageStateRequestedAsync,
    PageStatePending,
    PageStateEvicted,
};

struct TPage
    : public TSharedPageHandle
    , public TIntrusiveListItem<TPage>
{
    ui32 State : 4 = PageStateNo;
    ECacheMode CacheMode : 2 = ECacheMode::Regular;

    ES3FIFOPageLocation Location : 4 = ES3FIFOPageLocation::None;

    const TPageId PageId;
    const size_t Size;

    TCollection* Collection;

    TPage(TPageId pageId, size_t size, TCollection* collection)
        : PageId(pageId)
        , Size(size)
        , Collection(collection)
    {}

    bool HasMissingBody() const {
        switch (State) {
            case PageStateNo:
            case PageStateRequested:
            case PageStateRequestedAsync:
            case PageStatePending:
                return true;

            default:
                return false;
        }
    }

    void ProvideBody(TSharedData body) {
        Y_DEBUG_ABORT_UNLESS(HasMissingBody());
        TSharedPageHandle::Initialize(std::move(body));
        State = PageStateLoaded;
    }

    void EnsureNoCacheFlags() {
        Y_ENSURE(Location == ES3FIFOPageLocation::None, "Unexpected page " << Location << " Location");
    }
};

static_assert(sizeof(TPage) == 112);

}
