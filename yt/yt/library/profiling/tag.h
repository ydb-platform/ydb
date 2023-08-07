#pragma once

#include "public.h"

#include <util/generic/string.h>

#include <library/cpp/yt/small_containers/compact_vector.h>

#include <library/cpp/yt/memory/intrusive_ptr.h>

#include <vector>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

using TTagId = int;

constexpr int TypicalTagCount = 6;

using TTagIdList = TCompactVector<TTagId, TypicalTagCount>;

using TTag = std::pair<TString, TString>;

using TTagList = TCompactVector<TTag, TypicalTagCount>;

using TTagIndex = ui8;

using TTagIndexList = TCompactVector<TTagIndex, TypicalTagCount>;

constexpr ui8 NoTagSentinel = 0xff;

constexpr int NoParent = 0;

struct TDynamicTag final
{ };

using TDynamicTagPtr = TIntrusivePtr<TDynamicTag>;

class TProjectionSet
{
public:
    const TTagIndexList& Parents() const;
    const TTagIndexList& Children() const;
    const TTagIndexList& Required() const;
    const TTagIndexList& Excluded() const;
    const TTagIndexList& Alternative() const;

    const std::vector<std::pair<TDynamicTagPtr, TTagIndex>>& DynamicTags() const;

    template <class TFn>
    void Range(
        const TTagIdList& tags,
        TFn fn) const;

    void Resize(int size);
    void SetEnabled(bool enabled);

protected:
    bool Enabled_ = true;
    TTagIndexList Parents_;
    TTagIndexList Children_;
    TTagIndexList Required_;
    TTagIndexList Excluded_;
    TTagIndexList Alternative_;

    std::vector<std::pair<TDynamicTagPtr, TTagIndex>> DynamicTags_;
};

class TTagSet
    : public TProjectionSet
{
public:
    TTagSet() = default;
    explicit TTagSet(const TTagList& tags);

    TTagSet WithTag(TTag tag, int parent = NoParent) const;
    TTagSet WithRequiredTag(TTag tag, int parent = NoParent) const;
    TTagSet WithExcludedTag(TTag tag, int parent = NoParent) const;
    TTagSet WithAlternativeTag(TTag tag, int alternativeTo, int parent = NoParent) const;
    TTagSet WithExtensionTag(TTag tag, int extensionOf) const;
    TTagSet WithTagWithChild(TTag tag, int child) const;
    TTagSet WithTagSet(const TTagSet& other) const;

    void AddTag(TTag tag, int parent = NoParent);
    void AddRequiredTag(TTag tag, int parent = NoParent);
    void AddExcludedTag(TTag tag, int parent = NoParent);
    void AddAlternativeTag(TTag tag, int alternativeTo, int parent = NoParent);
    void AddExtensionTag(TTag tag, int extensionOf);
    void AddTagWithChild(TTag tag, int child);
    void Append(const TTagSet& other);

    TDynamicTagPtr AddDynamicTag(int index);

    const TTagList& Tags() const;

private:
    TTagList Tags_;
};

////////////////////////////////////////////////////////////////////////////////

template <class TFn>
void RangeSubsets(
    const TTagIdList& tags,
    const TTagIndexList& parents,
    const TTagIndexList& children,
    const TTagIndexList& required,
    const TTagIndexList& excluded,
    const TTagIndexList& alternative,
    TFn fn);

TTagIdList  operator +  (const TTagIdList& a, const TTagIdList& b);
TTagIdList& operator += (TTagIdList& a, const TTagIdList& b);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling

template <>
struct THash<NYT::NProfiling::TTagIndexList>
{
    size_t operator()(const NYT::NProfiling::TTagIndexList& ids) const;
};

template <>
struct THash<NYT::NProfiling::TTagList>
{
    size_t operator()(const NYT::NProfiling::TTagList& ids) const;
};

template <>
struct THash<NYT::NProfiling::TTagIdList>
{
    size_t operator()(const NYT::NProfiling::TTagIdList& ids) const;
};

#define TAG_INL_H_
#include "tag-inl.h"
#undef TAG_INL_H_

