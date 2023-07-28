#ifndef TAG_INL_H_
#error "Direct inclusion of this file is not allowed, include tag.h"
// For the sake of sane code completion.
#include "tag.h"
#endif
#undef TAG_INL_H_

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

inline const TTagIndexList& TProjectionSet::Parents() const
{
    return Parents_;
}

inline const TTagIndexList& TProjectionSet::Children() const
{
    return Children_;
}

inline const TTagIndexList& TProjectionSet::Required() const
{
    return Required_;
}

inline const TTagIndexList& TProjectionSet::Excluded() const
{
    return Excluded_;
}

inline const TTagIndexList& TProjectionSet::Alternative() const
{
    return Alternative_;
}

inline const std::vector<std::pair<TDynamicTagPtr, TTagIndex>>& TProjectionSet::DynamicTags() const
{
    return DynamicTags_;
}

////////////////////////////////////////////////////////////////////////////////

inline TTagSet::TTagSet(const TTagList& tags)
    : Tags_(tags)
{
    Resize(tags.size());
}

inline const TTagList& TTagSet::Tags() const
{
    return Tags_;
}

template <class TFn>
void TProjectionSet::Range(
    const TTagIdList& tags,
    TFn fn) const
{
    if (Enabled_) {
        RangeSubsets(tags, Parents_, Children_, Required_, Excluded_, Alternative_, fn);
    } else {
        fn(tags);
    }
}

////////////////////////////////////////////////////////////////////////////////

template <class TFn>
void RangeSubsets(
    const TTagIdList& tags,
    const TTagIndexList& parents,
    const TTagIndexList& children,
    const TTagIndexList& required,
    const TTagIndexList& excluded,
    const TTagIndexList& alternative,
    TFn fn)
{
    auto toMask = [] (auto list) {
        ui64 mask = 0;
        for (auto i : list) {
            mask |= 1 << i;
        }
        return mask;
    };

    ui64 requiredMask = toMask(required);
    ui64 excludedMask = toMask(excluded);
    YT_VERIFY(parents.size() == tags.size());

    for (ui64 mask = 0; mask < (1 << tags.size()); ++mask) {
        if ((mask & requiredMask) != requiredMask) {
            continue;
        }

        if ((mask & excludedMask) != 0) {
            continue;
        }

        bool skip = false;
        for (size_t i = 0; i < tags.size(); i++) {
            if (!(mask & (1 << i))) {
                if (children[i] != NoTagSentinel && (mask & (1 << children[i]))) {
                    skip = true;
                    break;
                }

                continue;
            }

            if (parents[i] != NoTagSentinel && !(mask & (1 << parents[i]))) {
                skip = true;
                break;
            }

            if (alternative[i] != NoTagSentinel && (mask & (1 << alternative[i]))) {
                skip = true;
                break;
            }
        }
        if (skip) {
            continue;
        }

        TTagIdList list;
        for (size_t i = 0; i < tags.size(); i++) {
            if (mask & (1 << i)) {
                list.push_back(tags[i]);
            }
        }

        fn(list);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
