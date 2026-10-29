#include "float8_load_ut.h"

#include <ydb/library/vector_distance/float8_distance_sse_impl.h>
#include <ydb/library/testlib/helpers.h>

#include <util/system/cpu_id.h>

Y_UNIT_TEST_SUITE(Float8Load) {
#ifdef ARCADIA_SSE
    Y_UNIT_TEST_TWIN(Sse, E5M2) {
        if (!NX86::HaveSSE2()) {
            return;
        }
        using T = std::conditional_t<E5M2, TFloat8E5M2, TFloat8E4M3>;
        using TOps = NVectorDistance::NFloat8::NSse::NPrivate::TOps;
        NVectorDistance::NFloat8::NTesting::TestLoad<T, TOps::Width>([](const T* ptr) {
            std::array<float, TOps::Width> result;
            _mm_storeu_ps(result.data(), TOps::Load(ptr));
            return result;
        });
    }
#endif

#ifdef TEST_FLOAT8_AVX2
    Y_UNIT_TEST_TWIN(Avx2, E5M2) {
        if (!NX86::HaveAVX2()) {
            return;
        }
        NVectorDistance::NFloat8::NTesting::TestAvx2Load(E5M2);
    }
#endif
}
