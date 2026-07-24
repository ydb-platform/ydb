#include <ydb/core/util/address_classifier.h>
#include <util/generic/string.h>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }

    const TString input(reinterpret_cast<const char*>(data), size);
    try {
        TString hostname;
        ui32 port = 0;
        NKikimr::NAddressClassifier::ParseAddress(input, hostname, port);
        (void)hostname;
        (void)port;
    } catch (...) {
    }
    return 0;
}
