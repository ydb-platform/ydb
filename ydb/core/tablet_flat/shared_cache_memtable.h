#pragma once

#include "defs.h"

namespace NKikimr {
namespace NSharedCache {

class ISharedPageCacheMemTableRegistration : TNonCopyable, public TThrRefBase {
public:
    virtual void SetConsumption(ui64 newConsumption) = 0;
    virtual ~ISharedPageCacheMemTableRegistration() = default;
};

class ISharedPageCacheMemTableObserver : TNonCopyable {
public:
    virtual void Register(ui32 table) = 0;
    virtual void Unregister(ui32 table) = 0;
    virtual void CompactionComplete(TIntrusivePtr<ISharedPageCacheMemTableRegistration> registration) = 0;
    virtual ~ISharedPageCacheMemTableObserver() = default;
};

}
}
