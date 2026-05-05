#pragma once

#include <ydb/library/actors/core/actorid.h>

#include <util/generic/string.h>
#include <util/string/builder.h>

#include <charconv>
#include <cstdint>
#include <cstring>
#include <string>

#include <type_traits>
#include <vector>

namespace NKikimr::NStructuredLog {

using TBinaryData = std::vector<std::uint8_t>;

template<typename T>
struct TNativeTypeSupport : public std::false_type{};

template<> struct TNativeTypeSupport<TString> : public std::true_type
{
    using TLength = std::size_t;

    static void Serialize(const TString& value, TBinaryData& data) {
        TLength valueLength = value.size();

        // Write contents
        auto oldSize = data.size();
        data.resize(oldSize + valueLength + sizeof(TLength));

        *(reinterpret_cast<TLength*>(data.data() + oldSize)) = valueLength;

        auto to = data.data() + oldSize + sizeof(TLength);
        std::memcpy(to, value.c_str(), valueLength);
    }

    static bool Deserialize(TString& value, const void* data, std::size_t length) {
        if (sizeof (TLength) > length) {
            return false;
        }

        TLength stringLength =  *(reinterpret_cast<const TLength*>(data));
        if (sizeof(TLength) + stringLength != length) {
            return false;
        }

        auto charPtr = reinterpret_cast<const char*>(data);
        value = TString(charPtr + sizeof(stringLength), stringLength);
        return true;
    }

    static TString ToString(const TString& value) {
        return value;
    }

    static void AppendToString(const TString& value, TStringBuilder& stringBuffer) {
        stringBuffer.append(value);
    }
};

}
