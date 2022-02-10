#include <library/cpp/monlib/encode/prometheus/prometheus.h>
#include <library/cpp/monlib/encode/fake/fake.h>

#include <util/stream/mem.h>


extern "C" int LLVMFuzzerTestOneInput(const ui8* buf, size_t size) {
    using namespace NMonitoring;

    try {
        TStringBuf data(reinterpret_cast<const char*>(buf), size);
        auto encoder = EncoderFake();
        DecodePrometheus(data, encoder.Get());
    } catch (...) {
    }

    return 0;
}
