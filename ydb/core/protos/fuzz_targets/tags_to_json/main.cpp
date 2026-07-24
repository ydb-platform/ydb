#include <ydb/core/ymq/base/helpers.h>
#include <library/cpp/json/writer/json_value.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>

namespace {

TString HexString(const uint8_t* data, size_t size) {
    static constexpr char HexDigits[] = "0123456789abcdef";
    TString out;
    out.reserve(size * 2);
    for (size_t i = 0; i < size; ++i) {
        const uint8_t byte = data[i];
        out.push_back(HexDigits[byte >> 4]);
        out.push_back(HexDigits[byte & 0x0F]);
    }
    return out;
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    NJson::TJsonMap tags;
    size_t pos = 0;
    for (size_t i = 0; i < 8 && pos < size; ++i) {
        const size_t keySize = std::min<size_t>((data[pos] % 4) + 1, size - pos);
        TString key = HexString(data + pos, keySize);
        pos += keySize;

        const uint8_t valueSeed = pos < size ? data[pos] : 0;
        const size_t valueSize = std::min<size_t>(valueSeed % 8, size - pos);
        TString value = HexString(data + pos, valueSize);
        pos += valueSize;

        tags[key] = NJson::TJsonValue(value);
    }

    try {
        TString json = NKikimr::NSQS::TagsToJson(tags);
        (void)json;
    } catch (...) {
    }

    return 0;
}
