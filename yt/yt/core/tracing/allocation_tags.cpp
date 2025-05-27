#include "allocation_tags.h"

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

TAllocationTagList::TAllocationTagList(TAllocationTags tags)
    : Tags_(std::move(tags))
{ }

const TAllocationTags& TAllocationTagList::GetTags() const noexcept
{
    return Tags_;
}

std::optional<TAllocationTagValue> TAllocationTagList::FindTagValue(const TAllocationTagKey& key) const
{
    for (const auto& [someKey, someValue] : Tags_) {
        if (someKey == key) {
            return someValue;
        }
    }
    return std::nullopt;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
