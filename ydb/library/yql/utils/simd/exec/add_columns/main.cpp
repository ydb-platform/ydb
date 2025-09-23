#include <util/generic/ptr.h>
#include <util/system/cpu_id.h>
#include <util/system/types.h>
#include <util/stream/output.h>
#include <util/generic/string.h>
#include <vector>
#include <immintrin.h>
#include <avxintrin.h>
#include <chrono>
#include <ydb/library/yql/utils/simd/simd.h>


const size_t size = 64e5;

template <typename T>
inline double GetSum(std::vector<std::vector<T>>& columns, std::vector<T>& result) {
    const size_t SIZE_OF_TYPE = 256 / (sizeof(T) * 8);
    const size_t align_size = columns[0].size();

    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

    for (size_t i = 0; i < align_size; i += SIZE_OF_TYPE) {
         NSimd::NAVX2::TSimd8<T> final_register(&columns[0][i]);

        for (size_t j = 1; j < columns.size(); ++j) {
            final_register.Add64(&columns[j][i]);
        }

        final_register.Store(&result[i]);
    }

    std::chrono::steady_clock::time_point finish = std::chrono::steady_clock::now();

    return std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();

}

double StandartAdding(std::vector<std::vector<ui64>>& columns, std::vector<ui64>& result) {
    std::chrono::steady_clock::time_point start = std::chrono::steady_clock::now();

    for (size_t j = 0; j < columns[0].size(); ++j) {

        for (size_t i = 0; i < columns[i].size(); ++i) {
            result[j] += columns[i][j];
        }

    }
    std::chrono::steady_clock::time_point finish = std::chrono::steady_clock::now();

    return std::chrono::duration_cast<std::chrono::microseconds>(finish - start).count();
}

int main() {
    std::vector<std::vector<ui64>> vec1(10, std::vector<ui64>(size, 1e12 + 3));

    std::vector<ui64> result1(size, 0);
    std::vector<ui64> result2(size, 0);

    double ans1 = GetSum(vec1, result1);
    double ans2 = StandartAdding(vec1, result2);

    for (size_t i = 0; i < result2.size(); ++i) {
        if (result2[i] != result1[i]) {
            Cerr << "something went wrong...";
            return 0;
        }
    }

    Cerr << "The results are the same. Let's compare times:\n";
    Cerr << "Time, using AVX2: " << ans1 << " ms\n";
    Cerr << "Time, using standart adding: " << ans2 << "ms";
}