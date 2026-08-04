#include <ydb/core/ymq/base/helpers.h>
#include <library/cpp/json/json_value.h>
#include <library/cpp/scheme/scheme.h>

#include <cstddef>
#include <cstdint>
#include <util/generic/string.h>

using namespace NKikimr::NSQS;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) {
        return 0;
    }

    try {
        TString json(reinterpret_cast<const char*>(data), size);
        NSc::TValue jsonValue = NSc::TValue::FromJson(json);
        if (jsonValue.IsDict()) {
            NJson::TJsonMap newTags;
            for (const auto& [k, v] : jsonValue.GetDict()) {
                if (v.IsString()) {
                    newTags[k] = NJson::TJsonValue(v.GetString());
                }
            }
            TMaybe<NJson::TJsonMap> currentTags; // Empty for simplicity
            TTagValidator validator(currentTags, newTags);
            (void)validator.Validate();
            (void)validator.GetJson();
            (void)validator.GetError();
        }
    } catch (...) {
    }

    return 0;
}
