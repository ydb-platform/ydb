#pragma once

#include <yt/yt/core/profiling/public.h>

#include <yt/yt/library/profiling/tag.h>
#include <yt/yt/library/profiling/solomon/sensor_dump.pb.h>

#include <library/cpp/monlib/encode/buffered/buffered_encoder_base.h>

#include <util/generic/hash_set.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TTagRegistry
{
public:
    TTagIdList Encode(const TTagSet& tags);
    TTagIdList Encode(const TTagList& tags);
    TTagId Encode(const TTag& tag);

    //! TryEncode returns null for an unknown tag.
    TCompactVector<std::optional<TTagId>, TypicalTagCount> TryEncode(const TTagList& tags) const;

    const TTag& Decode(TTagId tagId) const;
    int GetSize() const;
    THashMap<TString, int> GetTopByKey() const;

    void DumpTags(NProto::TSensorDump* dump);

private:
    // TODO(prime@): maybe do something about the fact that tags are never freed.
    THashMap<TTag, TTagId> TagByName_;
    std::deque<TTag> TagById_;

    THashMap<TTagId, TTagId> LegacyTags_;
};

////////////////////////////////////////////////////////////////////////////////

class TTagWriter
{
public:
    TTagWriter(const TTagRegistry& registry, ::NMonitoring::IMetricConsumer* encoder)
        : Registry_(registry)
        , Encoder_(encoder)
    { }

    void WriteLabel(TTagId tag);
    const TTag& Decode(TTagId tagId) const;

private:
    const TTagRegistry& Registry_;
    ::NMonitoring::IMetricConsumer* Encoder_;

    std::deque<std::optional<std::pair<ui32, ui32>>> Cache_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
