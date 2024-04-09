#include "mkql_vector_spiller_adapter.h"
#include "mock_spiller_ut.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {

    template <typename T>
    std::vector<T> CreateSimpleVectorOfSize(size_t size) {
        std::vector<T> v;
        v.reserve(size);

        for (size_t i = 0; i < size; ++i) {
            v.push_back(i);
        }

        return v;
    }

    template <typename T>
    void SaveRestoreAndCompareVectors(const std::vector<std::vector<T>>& vectors, size_t spillerChunkSizeInBytes) {
        auto spiller = TVectorSpillerAdapter<T>(CreateMockSpiller(), spillerChunkSizeInBytes);

        for (const auto& vec : vectors) {
            std::vector<T> copiedVector = vec;
            spiller.AddData(std::move(copiedVector));

            while (!spiller.IsAcceptingData()) {
                spiller.Update();
            }
        }

        spiller.Finalize();

        while (!spiller.IsAcceptingDataRequests()) {
            spiller.Update();
        }

        for (const auto& vec : vectors) {
            spiller.RequestNextVector();

            while (!spiller.IsDataReady()) {
                spiller.Update();
            }

            auto extractedVector = spiller.ExtractVector();

            UNIT_ASSERT_VALUES_EQUAL(vec, extractedVector);
        }   
    }

    template <typename T>
    void RunTestForSingleVector(size_t vectorSize, size_t chunkSize, bool sizeInBytes) {
        std::vector v = CreateSimpleVectorOfSize<T>(vectorSize);
        size_t chunkSizeInBytes = sizeInBytes ? chunkSize : chunkSize * sizeof(T);
        SaveRestoreAndCompareVectors<T>({v}, chunkSizeInBytes);
    }
}


Y_UNIT_TEST_SUITE(TVectorSpillerAdapterTest_SingleVector) {
    Y_UNIT_TEST(VectorOfExactChunkSize) {
        size_t vectorSize = 5;

        RunTestForSingleVector<int>(vectorSize, vectorSize, false);
        RunTestForSingleVector<char>(vectorSize, vectorSize, false);
    }

    Y_UNIT_TEST(VectorLargerThanChunkSize) {
        size_t vectorSize = 10;
        size_t chunkSize = 3;

        RunTestForSingleVector<int>(vectorSize, chunkSize, false);
        RunTestForSingleVector<char>(vectorSize, chunkSize, false);
    }

    Y_UNIT_TEST(VectorLargerThanChunkSizePrime) {
        size_t vectorSize = 10;
        size_t chunkSizeBytes = 7;

        RunTestForSingleVector<int>(vectorSize, chunkSizeBytes, true);
        RunTestForSingleVector<char>(vectorSize, chunkSizeBytes, true);
    }

    Y_UNIT_TEST(VectorLessThanChunkSize) {
        size_t vectorSize = 5;
        size_t chunkSize = 10;

        RunTestForSingleVector<int>(vectorSize, chunkSize, false);
        RunTestForSingleVector<char>(vectorSize, chunkSize, false);
    }

}

} //namespace namespace NKikimr::NMiniKQL
