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
    #define XX(name) std::atomic<size_t> name = 0;
    ENUMERATE_SLOT_FIELDS()
    #undef XX

    TGlobalSlot() = default;

    TGlobalSlot(TGlobalSlot&& other) noexcept
    {
        #define XX(name) name = other.name.load();
        ENUMERATE_SLOT_FIELDS()
        #undef XX
    }

    TGlobalSlot& operator+=(const TLocalSlot& rhs)
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

    std::string GetTypeName() const;
    std::string GetFullName() const;

    size_t GetObjectsAllocated() const;
    size_t GetObjectsFreed() const;
    size_t GetObjectsAlive() const;

    size_t GetBytesAllocated() const;
    size_t GetBytesFreed() const;
    size_t GetBytesAlive() const;

    TRefCountedTrackerStatistics::TNamedSlotStatistics GetStatistics() const;

    #define REF_COUNTED_TRACKER_NO_TSAN
    #if defined(__has_feature)
        #if __has_feature(thread_sanitizer)
            #undef REF_COUNTED_TRACKER_NO_TSAN
            #define REF_COUNTED_TRACKER_NO_TSAN __attribute__((no_sanitize("thread")))
        #endif
    #endif

    TNamedSlot& REF_COUNTED_TRACKER_NO_TSAN operator+=(const TLocalSlot& rhs)
    {
        #define XX(name) name ## _ += rhs.name;
        ENUMERATE_SLOT_FIELDS()
        #undef XX
        return *this;
    }

    #undef REF_COUNTED_TRACKER_NO_TSAN

    TNamedSlot& operator+=(const TGlobalSlot& rhs)
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

// The per-thread tracker state. These three fields used to live in three separate
// thread-locals; merging them into one struct lets the hot path (see
// INCREMENT_COUNTER) reach the bound and the slots pointer from a single TLS base.
struct TRefCountedTrackerLocalState
{
    // nullptr if not initialized or already destroyed
    TRefCountedTracker::TLocalSlots* Slots = nullptr;
    // nullptr if not initialized or already destroyed
    TRefCountedTracker::TLocalSlot* SlotsBegin = nullptr;
    //  0 if not initialized
    // -1 if already destroyed
    int SlotsSize = 0;
};

// The hot counters live in this constinit thread_local. It is read directly --
// not through the Y_NO_INLINE accessor that YT_DEFINE_THREAD_LOCAL would generate --
// so the local-exec TLS load (mov %fs:0x0 + offset) becomes a couple of inline
// instructions instead of a call. Fiber safety comes from the functions that
// perform the read -- TRefCountedTracker::{Allocate,Free}* below -- being marked
// YT_PREVENT_TLS_CACHING, so the thread pointer is re-read on every call rather than
// cached across a fiber migration. constinit guarantees static initialization,
// avoiding a per-access TLS-init guard for this cross-translation-unit thread_local.
extern constinit thread_local TRefCountedTrackerLocalState RefCountedTrackerLocalStateData;

Y_FORCE_INLINE TRefCountedTracker* TRefCountedTracker::Get()
{
    return LeakySingleton<TRefCountedTracker>();
}

// Inline implementations of the (private) hot counter functions: each reads the
// per-thread slots straight off the TLS base and bumps a counter. They are inlined
// into the TRefCountedTrackerFacade forwarders -- their only, friended caller -- which
// are pinned YT_PREVENT_TLS_CACHING (see ref_tracked.cpp). That out-of-line facade
// boundary is what makes inlining the local-exec TLS read here fiber-safe: the thread
// pointer is re-read on every call rather than cached across a fiber migration. Because
// these are private and friended only to the facade, no other context can inline the
// read and reintroduce that hazard.
#define INCREMENT_COUNTER(fallback, name, delta) \
    auto& state = RefCountedTrackerLocalStateData; \
    auto index = cookie.Underlying(); \
    YT_ASSERT(index >= 0); \
    if (index >= state.SlotsSize) [[unlikely]] { \
        Get()->fallback; \
    } else { \
        state.SlotsBegin[index].name += delta; \
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
