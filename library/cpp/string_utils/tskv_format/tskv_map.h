#pragma once

#include "escape.h"
#include <util/string/cast.h>
#include <util/string/split.h>

namespace NTskvFormat {
    namespace NDetail {
        void DeserializeKvToStringBufs(const TStringBuf& kv, TStringBuf& key, TStringBuf& value, TString& buffer, bool unescape);
        void DeserializeKvToStrings(const TStringBuf& kv, TString& key, TString& value, bool unescape);
    }

    template <typename T>
    TString& SerializeMap(const T& data, TString& result) {
        result.clear();
        for (const auto& kv : data) {
            if (result.size() > 0) {
                result.push_back('\t');
            }
            Escape(ToString(kv.first), result);
            result.push_back('=');
            Escape(ToString(kv.second), result);
        }
        return result;
    }

    /**
     * Deserializing to TStringBuf is faster, just remember that `data'
     * must not be invalidated while `result' is still in use.
     */
    template <typename T>
    void DeserializeMap(const TStringBuf& data, T& result, TString& buffer, bool unescape = true) {
        result.clear();
        buffer.clear();
        buffer.reserve(data.size());
        TStringBuf key, value;

        StringSplitter(data.begin(), data.end()).Split('\t').Consume([&](const TStringBuf kv){
            NDetail::DeserializeKvToStringBufs(kv, key, value, buffer, unescape);
            result[key] = value;
        });

        Y_ASSERT(buffer.size() <= data.size());
    }

    template <typename T>
    void DeserializeMap(const TStringBuf& data, T& result, bool unescape = true) {
        if constexpr(std::is_same<typename T::key_type, TStringBuf>::value ||
            std::is_same<typename T::mapped_type, TStringBuf>::value)
        {
            DeserializeMap(data, result, result.DeserializeBuffer, unescape); // we can't unescape values w/o buffer
            return;
        }
        result.clear();
        TString key, value;

        StringSplitter(data.begin(), data.end()).Split('\t').Consume([&](const TStringBuf kv){
            NDetail::DeserializeKvToStrings(kv, key, value, unescape);
            result[key] = value;
        });
    }
}
