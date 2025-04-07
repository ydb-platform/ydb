#include "tag_registry.h"

#include <yt/yt/core/misc/error.h>

#include <yt/yt/core/misc/protobuf_helpers.h>

#include <library/cpp/yt/assert/assert.h>

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

void TTagRegistry::SetLabelSanitizationPolicy(ELabelSanitizationPolicy labelSanitizationPolicy)
{
    LabelSanitizationPolicy_ = labelSanitizationPolicy;
}

TTagIdList TTagRegistry::Encode(const TTagSet& tags)
{
    return Encode(tags.Tags());
}

TTagIdList TTagRegistry::Encode(const TTagList& tags)
{
    TTagIdList ids;
    for (const auto& tag : tags) {
        ids.push_back(Encode(tag));
    }

    return ids;
}

TTagId TTagRegistry::Encode(const TTag& tag)
{
    if (LabelSanitizationPolicy_ == ELabelSanitizationPolicy::None) {
        return EncodeSanitized(tag);
    }

    if (auto sanitizeParameters = ScanForSanitize(tag.second);
        sanitizeParameters.IsSanitizationRequired())
    {
        return EncodeSanitized(SanitizeMonitoringTag(tag, sanitizeParameters.ResultingLength));
    } else {
        return EncodeSanitized(tag);
    }
}

TCompactVector<std::optional<TTagId>, TypicalTagCount> TTagRegistry::TryEncode(const TTagList& tags) const
{
    TCompactVector<std::optional<TTagId>, TypicalTagCount> ids;

    for (const auto& tag : tags) {
        if (LabelSanitizationPolicy_ == ELabelSanitizationPolicy::None) {
            ids.push_back(TryEncodeSanitized(tag));
            continue;
        }

        if (auto sanitizeParameters = ScanForSanitize(tag.second);
            sanitizeParameters.IsSanitizationRequired())
        {
            ids.push_back(TryEncodeSanitized(SanitizeMonitoringTag(tag, sanitizeParameters.ResultingLength)));
        } else {
            ids.push_back(TryEncodeSanitized(tag));
        }
    }

    return ids;
}

const TTag& TTagRegistry::Decode(TTagId tagId) const
{
    if (tagId < 1 || static_cast<size_t>(tagId) > TagById_.size()) {
        THROW_ERROR_EXCEPTION("Invalid tag")
            << TErrorAttribute("tag_id", tagId);
    }

    return TagById_[tagId - 1];
}

int TTagRegistry::GetSize() const
{
    return TagById_.size();
}

THashMap<std::string, int> TTagRegistry::GetTopByKey() const
{
    THashMap<std::string, int> counts;
    for (const auto& [key, value] : TagById_) {
        counts[key]++;
    }
    return counts;
}

void TTagRegistry::DumpTags(NProto::TSensorDump* dump)
{
    dump->add_tags();

    for (int i = 0; i < std::ssize(TagById_); i++) {
        auto* tag = dump->add_tags();
        tag->set_key(ToProto(TagById_[i].first));
        tag->set_value(ToProto(TagById_[i].second));
    }
}

////////////////////////////////////////////////////////////////////////////////

bool TTagRegistry::TSanitizeParameters::IsSanitizationRequired() const
{
    return ForbiddenCharCount > 0 || ResultingLength > MaxSolomonLabelSize;
}

////////////////////////////////////////////////////////////////////////////////

bool TTagRegistry::IsAllowedMonitoringTagValueChar(unsigned char c) const
{
    switch (LabelSanitizationPolicy_) {
        case ELabelSanitizationPolicy::Weak:
            return c != '\0';

        case ELabelSanitizationPolicy::Strong:
            return 31 < c &&
                c < 127 &&
                c != '|' &&
                c != '*' &&
                c != '?' &&
                c != '"' &&
                c != '\'' &&
                c != '\\' &&
                c != '`';

        default:
            YT_ABORT();
    }
}

TTagRegistry::TSanitizeParameters TTagRegistry::ScanForSanitize(const std::string& value) const
{
    YT_VERIFY(LabelSanitizationPolicy_ != ELabelSanitizationPolicy::None);

    int forbiddenCharCount = 0;
    for (unsigned char c : value) {
        forbiddenCharCount += static_cast<int>(!IsAllowedMonitoringTagValueChar(c));
    }

    return {
        .ForbiddenCharCount = forbiddenCharCount,
        .ResultingLength = static_cast<int>(value.size() + forbiddenCharCount * 2),
    };
}

std::string TTagRegistry::SanitizeMonitoringTagValue(const std::string& value, int resultingLength) const
{
    static constexpr int HalfMaxSolomonLabelSize = MaxSolomonLabelSize / 2;

    bool needTrim = resultingLength > MaxSolomonLabelSize;

    std::string result;
    result.resize(std::min(resultingLength, MaxSolomonLabelSize));

    int resultIndex = 0;
    for (int index = 0; resultIndex < (needTrim ? HalfMaxSolomonLabelSize : resultingLength); ++index) {
        unsigned char c = value[index];

        if (IsAllowedMonitoringTagValueChar(c)) {
            result[resultIndex++] = c;
        } else {
            result[resultIndex++] = '%';
            result[resultIndex++] = IntToHexLowercase[c >> 4];
            result[resultIndex++] = IntToHexLowercase[c & 0x0f];
        }
    }

    if (!needTrim) {
        return result;
    }

    resultIndex = MaxSolomonLabelSize - 1;
    for (int index = ssize(value) - 1; resultIndex > HalfMaxSolomonLabelSize + 2; --index) {
        unsigned char c = value[index];

        if (IsAllowedMonitoringTagValueChar(value[index])) {
            result[resultIndex--] = c;
        } else {
            result[resultIndex--] = IntToHexLowercase[c & 0x0f];
            result[resultIndex--] = IntToHexLowercase[c >> 4];
            result[resultIndex--] = '%';
        }
    }

    result[HalfMaxSolomonLabelSize] = '.';
    result[HalfMaxSolomonLabelSize + 1] = '.';
    result[HalfMaxSolomonLabelSize + 2] = '.';

    return result;
}

TTag TTagRegistry::SanitizeMonitoringTag(const TTag& tag, int resultingLength) const
{
    return {tag.first, SanitizeMonitoringTagValue(tag.second, resultingLength)};
}

std::optional<TTagId> TTagRegistry::TryEncodeSanitized(const TTag& tag) const
{
    if (auto it = TagByName_.find(tag); it != TagByName_.end()) {
        return it->second;
    } else {
        return std::nullopt;
    }
}

////////////////////////////////////////////////////////////////////////////////

void TTagWriter::WriteLabel(TTagId tag)
{
    if (static_cast<size_t>(tag) >= Cache_.size()) {
        Cache_.resize(tag + 1);
    }

    auto& translation = Cache_[tag];
    if (!translation) {
        const auto& tagStr = Registry_.Decode(tag);
        translation = Encoder_->PrepareLabel(tagStr.first, tagStr.second);
    }

    Encoder_->OnLabel(translation->first, translation->second);
}

const TTag& TTagWriter::Decode(TTagId tagId) const
{
    return Registry_.Decode(tagId);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
