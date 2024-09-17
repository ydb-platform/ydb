#pragma once

#include "public.h"

#include <yt/yt/core/misc/intrusive_mpsc_stack.h>

namespace NYT::NTracing {

////////////////////////////////////////////////////////////////////////////////

//! An immutable ref-counted list of allocation tags.
class TAllocationTagList
    : public TRefCounted
    , public TIntrusiveListItem<TAllocationTagList>
{
public:
    explicit TAllocationTagList(TAllocationTags tags);

    const TAllocationTags& GetTags() const noexcept;
    std::optional<TAllocationTagValue> FindTagValue(const TAllocationTagKey& key) const;

private:
    const TAllocationTags Tags_;
};

DEFINE_REFCOUNTED_TYPE(TAllocationTagList)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTracing
