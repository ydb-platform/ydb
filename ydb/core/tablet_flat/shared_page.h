#pragma once

#include "shared_handle.h"
#include "shared_cache_s3fifo.h"
#include "flat_page_iface.h"
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NSharedCache {

using TPageId = NTable::NPage::TPageId;
using TPageOffset = NTable::NPage::TPageOffset;
using TPageLocation = NTable::NPage::TPageLocation;
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

    EPage Type = EPage::Undef;
    ui32 Crc32 = 0;

    const TPageOffset Offset;
    const size_t Size;

    TCollection* Collection;

    TPage(TPageOffset offset, size_t size, EPage type, ui32 crc32, TCollection* collection)
        : Type(type)
        , Crc32(crc32)
        , Offset(offset)
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

static_assert(sizeof(TPage) == 120);

}
