#include <library/cpp/json/json_reader.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TStringBuf input((const char*)data, size);
    try {
        NJson::TJsonValue value;
        NJson::ReadJsonFastTree(input, &value, false, false);
    } catch (...) {}
    return 0;
}
