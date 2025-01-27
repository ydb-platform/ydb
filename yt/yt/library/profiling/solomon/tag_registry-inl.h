#ifndef TAG_REGISTRY_INL_H
#error "Direct inclusion of this file is not allowed, include tag_registry.h"
// For the sake of sane code completion.
#include "tag_registry.h"
#endif

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

template <class TTagPerfect>
TTagId TTagRegistry::EncodeSanitized(TTagPerfect&& tag)
{
    static_assert(std::is_same_v<std::remove_cvref_t<TTagPerfect>, TTag>);

    THashMap<TTag, TTagId>::insert_ctx insertCtx;
    if (auto it = TagByName_.find(tag, insertCtx); it != TagByName_.end()) {
        return it->second;
    } else {
        TTagId tagId = TagById_.size() + 1;

        TagByName_.emplace_direct(insertCtx, tag, tagId);
        TagById_.push_back(std::forward<TTagPerfect>(tag));

        return tagId;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
