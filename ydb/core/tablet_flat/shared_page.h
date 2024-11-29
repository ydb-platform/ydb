#pragma once

#include "defs.h"
#include "shared_handle.h"
#include <ydb/library/yverify_stream/yverify_stream.h>

namespace NKikimr::NSharedCache {

using TPageId = NTable::NPage::TPageId;

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
    ui32 CacheId : 4 = 0;
    ui32 CacheFlags1 : 4 = 0;
    ui32 CacheFlags2 : 4 = 0;

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

    void Initialize(TSharedData data) {
        Y_DEBUG_ABORT_UNLESS(HasMissingBody());
        TSharedPageHandle::Initialize(std::move(data));
        State = PageStateLoaded;
    }

    void EnsureNoCacheFlags() {
        Y_VERIFY_S(CacheId == 0, "Unexpected page " << CacheId << " cache id");
        Y_VERIFY_S(CacheFlags1 == 0, "Unexpected page " << CacheFlags1 << " cache flags 1");
        Y_VERIFY_S(CacheFlags2 == 0, "Unexpected page " << CacheFlags2 << " cache flags 2");
    }
};

static_assert(sizeof(TPage) == 104);

}
