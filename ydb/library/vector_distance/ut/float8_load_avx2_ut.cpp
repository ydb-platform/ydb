#include "float8_load_ut.h"

#include <ydb/library/vector_distance/float8_distance_avx2_impl.h>

namespace NVectorDistance::NFloat8::NTesting {

    namespace {
        template <typename T>
        void TestAvx2Load() {
            using TOps = NAvx2::NPrivate::TOps;
            TestLoad<T, TOps::Width>([](const T* ptr) {
                std::array<float, TOps::Width> result;
                _mm256_storeu_ps(result.data(), TOps::Load(ptr));
                return result;
            });
        }
    }

    void TestAvx2Load(bool e5m2) {
        if (e5m2) {
            TestAvx2Load<TFloat8E5M2>();
        } else {
            TestAvx2Load<TFloat8E4M3>();
        }
    }

} // namespace NVectorDistance::NFloat8::NTesting
