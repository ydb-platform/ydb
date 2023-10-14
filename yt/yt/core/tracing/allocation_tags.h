#pragma once

#include "public.h"

#include <library/cpp/yt/threading/spin_lock.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

class TAllocationTags : public TRefCounted
{
public:
    using TKey = TString;
    using TValue = TString;
    using TTags = std::vector<std::pair<TKey, TValue>>;

    explicit TAllocationTags(TTags tags);

    const TTags& GetTags() const noexcept;

    const TTags* GetTagsPtr() const noexcept;

    std::optional<TValue> FindTagValue(const TKey& key) const;

    static std::optional<TValue> FindTagValue(
        const TTags& tags,
        const TKey& key);

private:
    friend class TAllocationTagsFreeList;

    const TTags Tags_;
    TAllocationTags* Next_ = nullptr;
};

DEFINE_REFCOUNTED_TYPE(TAllocationTags)

class TAllocationTagsFreeList
{
public:
    //! Decreases refcount of tagsRawPtr. If refcount becomes zero, puts the pointer into queue.
    //!
    //! The intended usage is
    //! list->ScheduleFree(tags.Release());
    //! where tags is TAllocationTagsPtr.
    void ScheduleFree(TAllocationTags* tagsRawPtr);

    //! Free all the pointers in the queue.
    void Cleanup();

    ~TAllocationTagsFreeList();

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Spinlock_);
    TAllocationTags* Head_ = nullptr;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
