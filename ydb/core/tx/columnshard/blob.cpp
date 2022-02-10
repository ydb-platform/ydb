#include "blob.h"
#include "defs.h"

#include <ydb/core/tx/columnshard/engines/defs.h> 

#include <charconv>

namespace NKikimr::NOlap {

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
TUnifiedBlobId DoParseExtendedDsBlobId(const TString& s, TString& error) {
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
TUnifiedBlobId DoParseSmallBlobId(const TString& s, TString& error) {
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

TUnifiedBlobId TUnifiedBlobId::ParseFromString(const TString& str,
     const IBlobGroupSelector* dsGroupSelector, TString& error)
{
    if (str.size() <= 2) {
        error = "String size is too small";
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
        return DoParseExtendedDsBlobId(str, error);
    } else if (str[0] == 'S' && str[1] == 'M') {
        return DoParseSmallBlobId(str, error);
    } else if (str[0] == 'S' && str[1] == '3') {
        error = "S3 blob id parsing is not yet implemented";
        return TUnifiedBlobId();
    }

    error = Sprintf("Unknown blob id format: %c%c", str[0], str[1]);
    return TUnifiedBlobId();
}

}
