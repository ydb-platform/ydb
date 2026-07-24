#include <library/cpp/monlib/metrics/labels.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, size_t size) {
    TStringBuf input((const char*)data, size);
    try {
        NMonitoring::TLabel label;
        NMonitoring::TLabel::TryFromString(input, label);
    } catch (...) {}
    try {
        NMonitoring::TLabel label = NMonitoring::TLabel::FromString(input);
        (void)label;
    } catch (...) {}
    return 0;
}
