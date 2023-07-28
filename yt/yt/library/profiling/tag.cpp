#include "tag.h"

#include <library/cpp/yt/memory/new.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

TTagIdList operator + (const TTagIdList& a, const TTagIdList& b)
{
    auto result = a;
    result += b;
    return result;
}

TTagIdList& operator += (TTagIdList& a, const TTagIdList& b)
{
    a.insert(a.end(), b.begin(), b.end());
    return a;
}

////////////////////////////////////////////////////////////////////////////////

void TProjectionSet::Resize(int size)
{
    Parents_.resize(size, NoTagSentinel);
    Children_.resize(size, NoTagSentinel);
    Alternative_.resize(size, NoTagSentinel);
}

void TProjectionSet::SetEnabled(bool enabled)
{
    Enabled_ = enabled;
}

void TTagSet::Append(const TTagSet& other)
{
    auto offset = Tags_.size();

    for (const auto& tag : other.Tags_) {
        Tags_.push_back(tag);
    }

    for (auto i : other.Required_) {
        Required_.push_back(offset + i);
    }

    for (auto i : other.Excluded_) {
        Excluded_.push_back(offset + i);
    }

    for (auto i : other.Parents_) {
        if (i == NoTagSentinel) {
            Parents_.push_back(NoTagSentinel);
        } else {
            Parents_.push_back(i + offset);
        }
    }

    for (auto i : other.Children_) {
        if (i == NoTagSentinel) {
            Children_.push_back(NoTagSentinel);
        } else {
            Children_.push_back(i + offset);
        }
    }

    for (auto i : other.Alternative_) {
        if (i == NoTagSentinel) {
            Alternative_.push_back(NoTagSentinel);
        } else {
            Alternative_.push_back(i + offset);
        }
    }

    for (auto [tag, index] : other.DynamicTags_) {
        DynamicTags_.emplace_back(tag, index + offset);
    }
}

TTagSet TTagSet::WithTag(TTag tag, int parent) const
{
    auto copy = *this;
    copy.AddTag(std::move(tag), parent);
    return copy;
}

TTagSet TTagSet::WithRequiredTag(TTag tag, int parent) const
{
    auto copy = *this;
    copy.AddRequiredTag(std::move(tag), parent);
    return copy;
}

TTagSet TTagSet::WithExcludedTag(TTag tag, int parent) const
{
    auto copy = *this;
    copy.AddExcludedTag(std::move(tag), parent);
    return copy;
}

TTagSet TTagSet::WithAlternativeTag(TTag tag, int alternativeTo, int parent) const
{
    auto copy = *this;
    copy.AddAlternativeTag(std::move(tag), alternativeTo, parent);
    return copy;
}

TTagSet TTagSet::WithExtensionTag(TTag tag, int extensionOf) const
{
    auto copy = *this;
    copy.AddExtensionTag(std::move(tag), extensionOf);
    return copy;
}

TTagSet TTagSet::WithTagWithChild(TTag tag, int child) const
{
    auto copy = *this;
    copy.AddTagWithChild(std::move(tag), child);
    return copy;
}

TTagSet TTagSet::WithTagSet(const TTagSet& other) const
{
    auto copy = *this;
    copy.Append(other);
    return copy;
}

void TTagSet::AddTag(TTag tag, int parent)
{
    int parentIndex = Tags_.size() + parent;
    if (parentIndex >= 0 && static_cast<size_t>(parentIndex) < Tags_.size()) {
        Parents_.push_back(parentIndex);
    } else {
        Parents_.push_back(NoTagSentinel);
    }

    Children_.push_back(NoTagSentinel);
    Alternative_.push_back(NoTagSentinel);
    Tags_.emplace_back(std::move(tag));
}

void TTagSet::AddRequiredTag(TTag tag, int parent)
{
    Required_.push_back(Tags_.size());
    AddTag(std::move(tag), parent);
}

void TTagSet::AddExcludedTag(TTag tag, int parent)
{
    Excluded_.push_back(Tags_.size());
    AddTag(std::move(tag), parent);
}

void TTagSet::AddAlternativeTag(TTag tag, int alternativeTo, int parent)
{
    int alternativeIndex = Tags_.size() + alternativeTo;

    AddTag(std::move(tag), parent);

    if (alternativeIndex >= 0 && static_cast<size_t>(alternativeIndex) < Tags_.size()) {
        Alternative_.back() = alternativeIndex;
    }
}

void TTagSet::AddExtensionTag(TTag tag, int extensionOf)
{
    int extensionIndex = Tags_.size() + extensionOf;
    AddTag(std::move(tag), extensionOf);
    Children_.back() = extensionIndex;
}

void TTagSet::AddTagWithChild(TTag tag, int child)
{
    int childIndex = Tags_.size() + child;
    AddTag(tag);
    Children_.back() = childIndex;
}

TDynamicTagPtr TTagSet::AddDynamicTag(int index)
{
    auto tag = New<TDynamicTag>();
    DynamicTags_.emplace_back(tag, index);
    return tag;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

size_t THash<NYT::NProfiling::TTagIndexList>::operator()(const NYT::NProfiling::TTagIndexList& list) const
{
    size_t result = 0;
    for (auto index : list) {
        result = CombineHashes(result, std::hash<NYT::NProfiling::TTagIndex>()(index));
    }
    return result;
}

size_t THash<NYT::NProfiling::TTagList>::operator()(const NYT::NProfiling::TTagList& list) const
{
    size_t result = 0;
    for (const auto& tag : list) {
        result = CombineHashes(result, THash<NYT::NProfiling::TTag>()(tag));
    }
    return result;
}

size_t THash<NYT::NProfiling::TTagIdList>::operator()(const NYT::NProfiling::TTagIdList& list) const
{
    size_t result = 1;
    for (auto tag : list) {
        result = CombineHashes(result, std::hash<NYT::NProfiling::TTagId>()(tag));
    }
    return result;
}
