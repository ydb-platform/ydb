#include "native_types_support.h"

namespace NActors::NStructuredLog {

void TNativeTypeSupport<TString>::Serialize(const TString& value, TBinaryData& data) {
    TLength valueLength = value.size();

    // Write contents
    auto oldSize = data.size();
    data.resize(oldSize + valueLength + sizeof(TLength));

    auto to = data.data() + oldSize;
    memcpy(to, &valueLength, sizeof(TLength));

    to = data.data() + oldSize + sizeof(TLength);
    memcpy(to, value.data(), valueLength);
}

bool TNativeTypeSupport<TString>::Deserialize(TString& value, const void* data, std::size_t length) {
    if (sizeof(TLength) > length) {
        return false;
    }

    TLength stringLength;
    memcpy(&stringLength, data, sizeof(TLength));
    if (sizeof(TLength) + stringLength != length) {
        return false;
    }

    auto charPtr = static_cast<const char*>(data);
    value = TString(charPtr + sizeof(TLength), stringLength);
    return true;
}

TString TNativeTypeSupport<TString>::ToString(const TString& value) {
    return value;
}

void TNativeTypeSupport<TString>::AppendToString(const TString& value, TStringBuilder& stringBuffer) {
    stringBuffer.append(value);
}

}  // namespace NActors::NStructuredLog
