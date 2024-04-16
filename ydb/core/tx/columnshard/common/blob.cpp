#include "blob.h"
#include <ydb/core/tx/columnshard/common/protos/blob_range.pb.h>

#include <charconv>

namespace NKikimr::NOlap {

namespace {

#define PARSE_INT_COMPONENT(fieldType, fieldName, endChar) \
    if (pos >= endPos) { \
        error = "Failed to parse " #fieldName " component"; \
        return TUnifiedBlobId(); \
    } \
    fieldType fieldName = -1; \
    { \
        auto [ptr, ec] { std::from_chars(str + pos, str + endPos, fieldName) }; \
        if (ec != std::errc()) { \
            error = "Failed to parse " #fieldName " component"; \
            return TUnifiedBlobId(); \
        } else { \
            pos = ptr - str; \
        } \
        if (str[pos++] != endChar) { \
            error = #endChar " not found after " #fieldName; \
            return TUnifiedBlobId(); \
        } \
    }

// Format: "DS:group:logoBlobId"
// Example: "DS:2181038103:[72075186224038245:51:31595:2:0:11952:0]"
TUnifiedBlobId ParseExtendedDsBlobId(const TString& s, TString& error) {
    Y_ABORT_UNLESS(s.size() > 2);
    const char* str = s.c_str();
    Y_ABORT_UNLESS(str[0] == 'D' && str[1] == 'S');
    i64 pos = 2;
    i64 endPos = s.size();
    if (str[pos++] != ':') {
        error = "Starting ':' not found";
        return TUnifiedBlobId();
    }

    PARSE_INT_COMPONENT(ui32, dsGroup, ':');

    TLogoBlobID logoBlobId;
    if (!TLogoBlobID::Parse(logoBlobId, s.substr(pos), error)) {
        return TUnifiedBlobId();
    }

    return TUnifiedBlobId(dsGroup, logoBlobId);
}

}

TUnifiedBlobId TUnifiedBlobId::ParseFromString(const TString& str,
    const IBlobGroupSelector* dsGroupSelector, TString& error) {
    if (str.size() <= 2) {
        error = TStringBuilder() << "Wrong blob id: '" << str << "'";
        return TUnifiedBlobId();
    }

    if (str[0] == '[') {
        // If blobId starts with '[' this must be a logoblobId and if channel is set to FAKE_CHANNEL
        // this is a fake logoblobid used for small blob
        TLogoBlobID logoBlobId;
        bool parsed = TLogoBlobID::Parse(logoBlobId, str, error);
        if (!parsed) {
            error = "Cannot parse TLogoBlobID: " + error;
            return TUnifiedBlobId();
        }
        // DS blob
        if (!dsGroupSelector) {
            error = "Need TBlobGroupSelector to resolve DS group for the blob";
            return TUnifiedBlobId();
        }
        return TUnifiedBlobId(dsGroupSelector->GetGroup(logoBlobId), logoBlobId);
    } else if (str[0] == 'D' && str[1] == 'S') {
        return ParseExtendedDsBlobId(str, error);
    }

    error = TStringBuilder() << "Wrong blob id: '" << str << "'";
    return TUnifiedBlobId();
}

NKikimr::TConclusionStatus TBlobRange::DeserializeFromProto(const NKikimrColumnShardProto::TBlobRange& proto) {
    auto parsed = TUnifiedBlobId::BuildFromString(proto.GetBlobId(), nullptr);
    if (!parsed) {
        return parsed;
    }
    BlobId = parsed.DetachResult();

    Offset = proto.GetOffset();
    Size = proto.GetSize();
    return TConclusionStatus::Success();
}

NKikimr::TConclusion<NKikimr::NOlap::TBlobRange> TBlobRange::BuildFromProto(const NKikimrColumnShardProto::TBlobRange& proto) {
    TBlobRange result;
    auto parsed = result.DeserializeFromProto(proto);
    if (!parsed) {
        return parsed;
    } else {
        return result;
    }
}

NKikimrColumnShardProto::TBlobRange TBlobRange::SerializeToProto() const {
    NKikimrColumnShardProto::TBlobRange result;
    result.SetBlobId(BlobId.ToStringNew());
    result.SetOffset(Offset);
    result.SetSize(Size);
    return result;
}

}
