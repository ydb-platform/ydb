#include "string_transform.h"

#include <library/cpp/protobuf/runtime/escaping.h>

#include <library/cpp/string_utils/base64/base64.h>

namespace NProtobufJson {
    void TCEscapeTransform::Transform(TString& str) const {
        str = NProtoBufRuntime::CEscape(str);
    }

    void TSafeUtf8CEscapeTransform::Transform(TString& str) const {
        str = NProtoBufRuntime::Utf8SafeCEscape(str);
    }

    void TDoubleEscapeTransform::Transform(TString& str) const {
        TString escaped = NProtoBufRuntime::CEscape(str);
        str = "";
        for (char* it = escaped.begin(); *it; ++it) {
            if (*it == '\\' || *it == '\"')
                str += "\\";
            str += *it;
        }
    }

    void TDoubleUnescapeTransform::Transform(TString& str) const {
        str = NProtoBufRuntime::UnescapeCEscapeString(Unescape(str));
    }

    TString TDoubleUnescapeTransform::Unescape(const TString& str) const {
        if (str.empty()) {
            return str;
        }

        TString result;
        result.reserve(str.size());

        char prev = str[0];
        bool doneOutput = true;
        for (const char* it = str.c_str() + 1; *it; ++it) {
            if (doneOutput && prev == '\\' && (*it == '\\' || *it == '\"')) {
                doneOutput = false;
            } else {
                result += prev;
                doneOutput = true;
            }
            prev = *it;
        }

        if ((doneOutput && prev != '\\') || !doneOutput) {
            result += prev;
        }

        return result;
    }

    void TBase64EncodeBytesTransform::TransformBytes(TString &str) const {
        str = Base64Encode(str);
    }

    void TBase64DecodeBytesTransform::TransformBytes(TString &str) const {
        str = Base64Decode(str);
    }
}
