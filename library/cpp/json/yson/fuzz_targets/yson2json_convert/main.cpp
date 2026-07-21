#include <library/cpp/json/yson/json2yson.h>
#include <library/cpp/json/json_value.h>
#include <util/generic/string.h>
#include <util/stream/str.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TStringBuf input((const char*)data, size);
    try {
        TString result = NJson2Yson::ConvertYson2Json(input);
        (void)result;
    } catch (...) {}
    try {
        NJson::TJsonValue value;
        NJson2Yson::DeserializeYsonAsJsonValue(input, &value, false);
    } catch (...) {}
    return 0;
}
