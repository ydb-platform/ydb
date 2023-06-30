#include "allocation_tags.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

TAllocationTags::TAllocationTags(std::vector<std::pair<TString, TString>> tags)
    : Tags_(std::move(tags))
{ }

const TAllocationTags::TTags& TAllocationTags::GetTags() const
{
    return Tags_;
}

TAllocationTagsFreeList::~TAllocationTagsFreeList()
{
    Cleanup();
}

void TAllocationTagsFreeList::ScheduleFree(TAllocationTags* tagsRawPtr)
{
    if (tagsRawPtr == nullptr) {
        return;
    }
    if (!GetRefCounter(tagsRawPtr)->Unref()) {
        return;
    }
    YT_VERIFY(tagsRawPtr->Next_ == nullptr);
    auto guard = Guard(Spinlock_);
    tagsRawPtr->Next_ = Head_;
    Head_ = tagsRawPtr;
}

void TAllocationTagsFreeList::Cleanup()
{
    auto guard = Guard(Spinlock_);
    auto head = std::exchange(Head_, nullptr);
    guard.Release();
    while (head != nullptr) {
        auto oldHead = head;
        head = head->Next_;
        DestroyRefCounted(oldHead);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
