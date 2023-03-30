#include "blob.h"
#include "defs.h"

#include <charconv>

namespace NKikimr::NOlap {

// Format: "S3-f(logoBlobId)-group"
// Example: "S3-42-72075186224038245_51_31595_2_0_11952_0-2181038103"
TString DsIdToS3Key(const TUnifiedBlobId& dsid, const ui64 pathId) {
    TString blobId = dsid.GetLogoBlobId().ToString();
    for (auto&& c : blobId) {
        switch (c) {
            case ':':
                c = '_';
                break;
            case '[':
            case ']':
                c = '-';
        }
    }
    TString result =
        "S3-" +
        ::ToString(pathId) +
        blobId +
        ::ToString(dsid.GetDsGroup())
        ;
    return result;
}

TUnifiedBlobId S3KeyToDsId(const TString& s, TString& error, ui64& pathId) {
    TVector<TString> keyBucket;
    Split(s, "-", keyBucket);

    ui32 dsGroup;
    if (keyBucket.size() != 4 || keyBucket[0] != "S3"
        || !TryFromString<ui32>(keyBucket[3], dsGroup)
        || !TryFromString<ui64>(keyBucket[1], pathId))
    {
        error = TStringBuilder() << "Wrong S3 key '" << s << "'";
        return TUnifiedBlobId();
    }

    TString blobId = "[" + keyBucket[2] + "]";
    for (size_t i = 0; i < blobId.size(); ++i) {
        switch (blobId[i]) {
            case '_':
                blobId[i] = ':';
                break;
        }
    }

    TLogoBlobID logoBlobId;
    if (!TLogoBlobID::Parse(logoBlobId, blobId, error)) {
        return TUnifiedBlobId();
    }

    return TUnifiedBlobId(dsGroup, logoBlobId);
}

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
    Y_VERIFY(s.size() > 2);
    const char* str = s.c_str();
    Y_VERIFY(str[0] == 'D' && str[1] == 'S');
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

// Format: "SM[tabletId:generation:step:cookie:size]"
// Example: "SM[72075186224038245:51:31184:0:2528]"
TUnifiedBlobId ParseSmallBlobId(const TString& s, TString& error) {
    Y_VERIFY(s.size() > 2);
    const char* str = s.c_str();
    Y_VERIFY(str[0] == 'S' && str[1] == 'M');
    i64 pos = 2;
    i64 endPos = s.size();
    if (str[pos++] != '[') {
        error = "opening [ not found";
        return TUnifiedBlobId();
    }

    PARSE_INT_COMPONENT(ui64, tabletId, ':');
    PARSE_INT_COMPONENT(ui32, gen, ':');
    PARSE_INT_COMPONENT(ui32, step, ':');
    PARSE_INT_COMPONENT(ui32, cookie, ':');
    PARSE_INT_COMPONENT(ui32, size, ']');

    if (pos != endPos) {
        error = "Extra characters after closing ]";
        return TUnifiedBlobId();
    }

    return TUnifiedBlobId(tabletId, gen, step, cookie, size);
}

// Format: "s = S3_key"
TUnifiedBlobId ParseS3BlobId(const TString& s, TString& error) {
    ui64 pathId;
    TUnifiedBlobId dsBlobId = S3KeyToDsId(s, error, pathId);
    if (!dsBlobId.IsValid()) {
        return TUnifiedBlobId();
    }

    return TUnifiedBlobId(dsBlobId, TUnifiedBlobId::S3_BLOB, pathId);
}

}

TUnifiedBlobId TUnifiedBlobId::ParseFromString(const TString& str,
     const IBlobGroupSelector* dsGroupSelector, TString& error)
{
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
        if (logoBlobId.Channel() == TSmallBlobId::FAKE_CHANNEL) {
            // Small blob
            return TUnifiedBlobId(logoBlobId.TabletID(), logoBlobId.Generation(), logoBlobId.Step(),
                logoBlobId.Cookie(), logoBlobId.BlobSize());
        } else {
            // DS blob
            if (!dsGroupSelector) {
                error = "Need TBlobGroupSelector to resolve DS group for the blob";
                return TUnifiedBlobId();
            }
            return TUnifiedBlobId(dsGroupSelector->GetGroup(logoBlobId), logoBlobId);
        }
    } else if (str[0] == 'D' && str[1] == 'S') {
        return ParseExtendedDsBlobId(str, error);
    } else if (str[0] == 'S' && str[1] == 'M') {
        return ParseSmallBlobId(str, error);
    } else if (str[0] == 'S' && str[1] == '3') {
        return ParseS3BlobId(str, error);
    }

    error = TStringBuilder() << "Wrong blob id: '" << str << "'";
    return TUnifiedBlobId();
}

}
