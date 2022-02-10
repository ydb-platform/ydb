#include <library/cpp/monlib/encode/spack/spack_v1.h>
#include <library/cpp/monlib/encode/fake/fake.h>

#include <util/stream/mem.h>


extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    using namespace NMonitoring;

    TMemoryInput min{data, size};

    auto encoder = EncoderFake();

    try {
        DecodeSpackV1(&min, encoder.Get());
    } catch (...) {
    }

    return 0;
}
