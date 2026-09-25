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

TString TNativeTypeSupport<bool>::ToString(const bool& value) {
    return value?"true":"false";
}

void TNativeTypeSupport<bool>::AppendToString(const bool& value, TStringBuilder& stringBuffer) {
    stringBuffer << ToString(value);
}

void TNativeTypeSupport<TInstant>::Serialize(const TInstant& value, TBinaryData& data) {
    ui64 microSeconds = value.MicroSeconds();
    TNativeTypeSupport<ui64>::Serialize(microSeconds, data);
}

bool TNativeTypeSupport<TInstant>::Deserialize(TInstant& value, const void* data, std::size_t length) {
    ui64 microSeconds;
    if (!TNativeTypeSupport<ui64>::Deserialize(microSeconds, data, length)) {
        return false;
    }
    value = TInstant::MicroSeconds(microSeconds);
    return true;
}

TString TNativeTypeSupport<TInstant>::ToString(const TInstant& value) {
    return value.ToString();
}

void TNativeTypeSupport<TInstant>::AppendToString(const TInstant& value, TStringBuilder& stringBuffer) {
    stringBuffer << ToString(value);
}


}  // namespace NActors::NStructuredLog
