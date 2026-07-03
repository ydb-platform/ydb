#pragma once

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <vector>

namespace NActors::NStructuredLog {

using TBinaryData = std::vector<std::uint8_t>;

template <typename T>
struct TNativeTypeSupport : public std::false_type {};

template <>
struct TNativeTypeSupport<TString> : public std::true_type {
    using TLength = std::size_t;

    static void Serialize(const TString& value, TBinaryData& data);

    static bool Deserialize(TString& value, const void* data, std::size_t length);

    static TString ToString(const TString& value);

    static void AppendToString(const TString& value, TStringBuilder& stringBuffer);
};

}  // namespace NActors::NStructuredLog
