#ifndef REF_COUNTED_TRACKER_INL_H_
#error "Direct inclusion of this file is not allowed, include ref_counted_tracker.h"
// For the sake of sane code completion.
#include "ref_counted_tracker.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

#define ENUMERATE_SLOT_FIELDS() \
    XX(ObjectsAllocated) \
    XX(ObjectsFreed) \
    XX(TagObjectsAllocated) \
    XX(TagObjectsFreed) \
    XX(SpaceSizeAllocated) \
    XX(SpaceSizeFreed)

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedTracker::TLocalSlot
{
    #define XX(name) size_t name = 0;
    ENUMERATE_SLOT_FIELDS()
    #undef XX
};

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedTracker::TGlobalSlot
{
    #define XX(name) std::atomic<size_t> name = {0};
    ENUMERATE_SLOT_FIELDS()
    #undef XX

    TGlobalSlot() = default;

    TGlobalSlot(TGlobalSlot&& other)
    {
        #define XX(name) name = other.name.load();
        ENUMERATE_SLOT_FIELDS()
        #undef XX
    }

    TGlobalSlot& operator += (const TLocalSlot& rhs)
    {
        #define XX(name) name += rhs.name;
        ENUMERATE_SLOT_FIELDS()
        #undef XX
        return *this;
    }
};

////////////////////////////////////////////////////////////////////////////////

class TRefCountedTracker::TNamedSlot
{
public:
    TNamedSlot(const TKey& key, size_t objectSize);

    TRefCountedTypeKey GetTypeKey() const;
    const TSourceLocation& GetLocation() const;

    TString GetTypeName() const;
    TString GetFullName() const;

    size_t GetObjectsAllocated() const;
    size_t GetObjectsFreed() const;
    size_t GetObjectsAlive() const;

    size_t GetBytesAllocated() const;
    size_t GetBytesFreed() const;
    size_t GetBytesAlive() const;

    TRefCountedTrackerStatistics::TNamedSlotStatistics GetStatistics() const;

    TNamedSlot& operator += (const TLocalSlot& rhs)
    {
        #define XX(name) name ## _ += rhs.name;
        ENUMERATE_SLOT_FIELDS()
        #undef XX
        return *this;
    }

    TNamedSlot& operator += (const TGlobalSlot& rhs)
    {
        #define XX(name) name ## _ += rhs.name.load();
        ENUMERATE_SLOT_FIELDS()
        #undef XX
        return *this;
    }

private:
    TKey Key_;
    size_t ObjectSize_;

    size_t ObjectsAllocated_ = 0;
    size_t ObjectsFreed_ = 0;
    size_t TagObjectsAllocated_ = 0;
    size_t TagObjectsFreed_ = 0;
    size_t SpaceSizeAllocated_ = 0;
    size_t SpaceSizeFreed_ = 0;

    static size_t ClampNonnegative(size_t allocated, size_t freed);
};

#undef ENUMERATE_SLOT_FIELDS

////////////////////////////////////////////////////////////////////////////////

YT_DECLARE_THREAD_LOCAL(TRefCountedTracker::TLocalSlot*, RefCountedTrackerLocalSlotsBegin);
YT_DECLARE_THREAD_LOCAL(int, RefCountedTrackerLocalSlotsSize);

Y_FORCE_INLINE TRefCountedTracker* TRefCountedTracker::Get()
{
    return LeakySingleton<TRefCountedTracker>();
}

#define INCREMENT_COUNTER(fallback, name, delta) \
    auto index = cookie.Underlying(); \
    YT_ASSERT(index >= 0); \
    if (Y_UNLIKELY(index >= RefCountedTrackerLocalSlotsSize())) { \
        Get()->fallback; \
    } else { \
        RefCountedTrackerLocalSlotsBegin()[index].name += delta; \
    }

Y_FORCE_INLINE void TRefCountedTracker::AllocateInstance(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER(AllocateInstanceSlow(cookie), ObjectsAllocated, 1)
}

Y_FORCE_INLINE void TRefCountedTracker::FreeInstance(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER(FreeInstanceSlow(cookie), ObjectsFreed, 1)
}

Y_FORCE_INLINE void TRefCountedTracker::AllocateTagInstance(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER(AllocateTagInstanceSlow(cookie), TagObjectsAllocated, 1)
}

Y_FORCE_INLINE void TRefCountedTracker::FreeTagInstance(TRefCountedTypeCookie cookie)
{
    INCREMENT_COUNTER(FreeTagInstanceSlow(cookie), TagObjectsFreed, 1)
}

Y_FORCE_INLINE void TRefCountedTracker::AllocateSpace(TRefCountedTypeCookie cookie, size_t space)
{
    INCREMENT_COUNTER(AllocateSpaceSlow(cookie, space), SpaceSizeAllocated, space)
}

Y_FORCE_INLINE void TRefCountedTracker::FreeSpace(TRefCountedTypeCookie cookie, size_t space)
{
    INCREMENT_COUNTER(FreeSpaceSlow(cookie, space), SpaceSizeFreed, space)
}

#undef INCREMENT_COUNTER

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
