#pragma once
#include "defs.h"
#include "shared_handle.h"

namespace NKikimr::NSharedCache {

class TSharedCachePages : public TThrRefBase {
public:
    TIntrusivePtr<TSharedPageGCList> GCList = new TSharedPageGCList;
};

}
