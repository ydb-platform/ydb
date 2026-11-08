#include "int4_load_ut.h"
#include <ydb/library/vector_distance/int4_distance_avx2_impl.h>

namespace NVectorDistance::NInt4::NTesting {
    std::array<float, 8> LoadAvx2(const ui8* ptr, std::false_type) {
        std::array<float, 8> result;
        _mm256_storeu_ps(result.data(), NAvx2::NPrivate::TOps<false>::Load(ptr));
        return result;
    }
    std::array<float, 8> LoadAvx2(const ui8* ptr, std::true_type) {
        std::array<float, 8> result;
        _mm256_storeu_ps(result.data(), NAvx2::NPrivate::TOps<true>::Load(ptr));
        return result;
    }
}
