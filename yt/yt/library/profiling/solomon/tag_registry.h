#pragma once

#include "public.h"

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
    void SetLabelSanitizationPolicy(ELabelSanitizationPolicy labelSanitizationPolicy);

    TTagIdList Encode(const TTagSet& tags);
    TTagIdList Encode(const TTagList& tags);
    TTagId Encode(const TTag& tag);

    //! TryEncode returns null for an unknown tag.
    TCompactVector<std::optional<TTagId>, TypicalTagCount> TryEncode(const TTagList& tags) const;

    const TTag& Decode(TTagId tagId) const;
    int GetSize() const;
    THashMap<std::string, int> GetTopByKey() const;

    void DumpTags(NProto::TSensorDump* dump);

private:
    struct TSanitizeParameters
    {
        int ForbiddenCharCount;
        int ResultingLength;

        bool IsSanitizationRequired() const;
    };

    bool IsAllowedMonitoringTagValueChar(unsigned char c) const;
    TSanitizeParameters ScanForSanitize(const std::string& value) const;
    std::string SanitizeMonitoringTagValue(const std::string& value, int resultingLength) const;
    TTag SanitizeMonitoringTag(const TTag& tag, int resultingLength) const;

    template <class TTagPerfect>
    TTagId EncodeSanitized(TTagPerfect&& tag);
    std::optional<TTagId> TryEncodeSanitized(const TTag& tag) const;

    ELabelSanitizationPolicy LabelSanitizationPolicy_ = ELabelSanitizationPolicy::None;
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

#define TAG_REGISTRY_INL_H
#include "tag_registry-inl.h"
#undef TAG_REGISTRY_INL_H
