#pragma once

#include "public.h"
#include "singleton.h"

#include <library/cpp/yt/threading/fork_aware_spin_lock.h>

#include <library/cpp/yt/misc/tls.h>
#include <library/cpp/yt/misc/source_location.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TRefCountedTrackerStatistics
{
    struct TStatistics
    {
        size_t ObjectsAllocated = 0;
        size_t ObjectsFreed = 0;
        size_t ObjectsAlive = 0;
        size_t BytesAllocated = 0;
        size_t BytesFreed = 0;
        size_t BytesAlive = 0;

        TStatistics& operator+= (const TStatistics& rhs);
    };

    struct TNamedSlotStatistics
        : public TStatistics
    {
        TString FullName;
    };

    std::vector<TNamedSlotStatistics> NamedStatistics;
    TStatistics TotalStatistics;
};

////////////////////////////////////////////////////////////////////////////////

// Reference tracking relies on uniqueness of std::type_info objects.
// Without uniqueness reference tracking is still functional but lacks precision
// (i. e. some types may have duplicate slots in the accumulated table).
// GCC guarantees std::type_info uniqueness starting from version 3.0
// due to the so-called vague linking.
//
// See also: http://gcc.gnu.org/faq.html#dso
// See also: http://www.codesourcery.com/public/cxx-abi/
class TRefCountedTracker
    : private TNonCopyable
{
public:
    struct TLocalSlot;
    using TLocalSlots = std::vector<TLocalSlot>;

    struct TGlobalSlot;
    using TGlobalSlots = std::vector<TGlobalSlot>;

    class TNamedSlot;
    using TNamedStatistics = std::vector<TNamedSlot>;

    static TRefCountedTracker* Get();

    TRefCountedTypeCookie GetCookie(
        TRefCountedTypeKey typeKey,
        size_t objectSize,
        const TSourceLocation& location = TSourceLocation());

    static void AllocateInstance(TRefCountedTypeCookie cookie);
    static void FreeInstance(TRefCountedTypeCookie cookie);

    static void AllocateTagInstance(TRefCountedTypeCookie cookie);
    static void FreeTagInstance(TRefCountedTypeCookie cookie);

    static void AllocateSpace(TRefCountedTypeCookie cookie, size_t size);
    static void FreeSpace(TRefCountedTypeCookie cookie, size_t size);

    TString GetDebugInfo(int sortByColumn = -1) const;
    TRefCountedTrackerStatistics GetStatistics() const;

    size_t GetObjectsAllocated(TRefCountedTypeKey typeKey) const;
    size_t GetObjectsAlive(TRefCountedTypeKey typeKey) const;
    size_t GetBytesAllocated(TRefCountedTypeKey typeKey) const;
    size_t GetBytesAlive(TRefCountedTypeKey typeKey) const;

    int GetTrackedThreadCount() const;

private:
    DECLARE_LEAKY_SINGLETON_FRIEND()

    struct TLocalSlotsHolder;

    struct TKey
    {
        TRefCountedTypeKey TypeKey;
        TSourceLocation Location;

        bool operator < (const TKey& other) const;
        bool operator == (const TKey& other) const;
    };

    mutable NThreading::TForkAwareSpinLock SpinLock_;
    std::map<TKey, TRefCountedTypeCookie> KeyToCookie_;
    std::map<TRefCountedTypeKey, size_t> TypeKeyToObjectSize_;
    std::vector<TKey> CookieToKey_;
    std::vector<TGlobalSlot> GlobalSlots_;
    THashSet<TLocalSlots*> AllLocalSlots_;

    TNamedStatistics GetSnapshot() const;
    static void SortSnapshot(TNamedStatistics* snapshot, int sortByColumn);

    size_t GetObjectSize(TRefCountedTypeKey typeKey) const;

    TNamedSlot GetSlot(TRefCountedTypeKey typeKey) const;

    void AllocateInstanceSlow(TRefCountedTypeCookie cookie);
    void FreeInstanceSlow(TRefCountedTypeCookie cookie);

    void AllocateTagInstanceSlow(TRefCountedTypeCookie cookie);
    void FreeTagInstanceSlow(TRefCountedTypeCookie cookie);

    void AllocateSpaceSlow(TRefCountedTypeCookie cookie, size_t size);
    void FreeSpaceSlow(TRefCountedTypeCookie cookie, size_t size);

    TLocalSlot* GetLocalSlot(TRefCountedTypeCookie cookie);
    TGlobalSlot* GetGlobalSlot(TRefCountedTypeCookie cookie);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define REF_COUNTED_TRACKER_INL_H_
#include "ref_counted_tracker-inl.h"
#undef REF_COUNTED_TRACKER_INL_H_
