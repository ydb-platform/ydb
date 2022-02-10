#include "utils.h"

#include <library/cpp/string_utils/base64/base64.h>

namespace NScUtils {

void CopyField(const NSc::TValue& from, NSc::TValue& to) {
    to = from;
}

} // namespace NScUtils

void TSerializer<NSc::TValue>::Save(IOutputStream* out, const NSc::TValue& v) {
    TString json = Base64Encode(v.ToJson());
    ::Save(out, json);
}

void TSerializer<NSc::TValue>::Load(IInputStream* in, NSc::TValue& v) {
    TString json;
    ::Load(in, json);
    json = Base64Decode(json);
    v = NSc::TValue::FromJsonThrow(json);
}
