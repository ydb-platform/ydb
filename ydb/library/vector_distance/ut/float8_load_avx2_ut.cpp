#include "float8_load_ut.h"

#include <ydb/library/vector_distance/float8_distance_avx2_impl.h>

namespace NVectorDistance::NFloat8::NTesting {

    namespace {
        template <typename T>
        std::array<float, 8> LoadAvx2Impl(const T* ptr) {
            std::array<float, 8> result;
            _mm256_storeu_ps(result.data(), NAvx2::NPrivate::TOps::Load(ptr));
            return result;
        }
    }

    std::array<float, 8> LoadAvx2(const TFloat8E4M3* ptr) {
        return LoadAvx2Impl(ptr);
    }

    std::array<float, 8> LoadAvx2(const TFloat8E5M2* ptr) {
        return LoadAvx2Impl(ptr);
    }

} // namespace NVectorDistance::NFloat8::NTesting
