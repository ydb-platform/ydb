#include <library/cpp/monlib/encode/json/json.h>
#include <library/cpp/monlib/encode/fake/fake.h>

#include <util/generic/strbuf.h>


extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    auto encoder = NMonitoring::EncoderFake();

    try {
        NMonitoring::DecodeJson({reinterpret_cast<const char*>(data), size}, encoder.Get());
    } catch (...) {
    }

    return 0;
}
