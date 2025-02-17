#include "mkql_vector_spiller_adapter.h"
#include "mock_spiller_ut.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMiniKQL {

namespace {

    template <typename T>
    std::vector<T, TMKQLAllocator<T>> CreateSimpleVectorOfSize(size_t size) {
        std::vector<T, TMKQLAllocator<T>> v;
        v.reserve(size);

        for (size_t i = 0; i < size; ++i) {
            v.push_back(i);
        }

        return v;
    }

    template <typename T>
    void SaveRestoreAndCompareVectors(const std::vector<std::vector<T, TMKQLAllocator<T>>>& vectors, size_t spillerChunkSizeInBytes) {
        auto spiller = TVectorSpillerAdapter<T, TMKQLAllocator<T>>(CreateMockSpiller(), spillerChunkSizeInBytes);

        for (const auto& vec : vectors) {
            auto copiedVector = vec;
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
        TScopedAlloc Alloc(__LOCATION__);
        size_t vectorSize = 5;

        RunTestForSingleVector<int>(vectorSize, vectorSize, false);
        RunTestForSingleVector<char>(vectorSize, vectorSize, false);
    }

    Y_UNIT_TEST(VectorLargerThanChunkSize) {
        TScopedAlloc Alloc(__LOCATION__);
        size_t vectorSize = 10;
        size_t chunkSize = 3;

        RunTestForSingleVector<int>(vectorSize, chunkSize, false);
        RunTestForSingleVector<char>(vectorSize, chunkSize, false);
    }

    Y_UNIT_TEST(VectorLargerThanChunkSizePrime) {
        TScopedAlloc Alloc(__LOCATION__);
        size_t vectorSize = 10;
        size_t chunkSizeBytes = 7;

        RunTestForSingleVector<int>(vectorSize, chunkSizeBytes, true);
        RunTestForSingleVector<char>(vectorSize, chunkSizeBytes, true);
    }

    Y_UNIT_TEST(VectorLessThanChunkSize) {
        TScopedAlloc Alloc(__LOCATION__);
        size_t vectorSize = 5;
        size_t chunkSize = 10;

        RunTestForSingleVector<int>(vectorSize, chunkSize, false);
        RunTestForSingleVector<char>(vectorSize, chunkSize, false);
    }
}

Y_UNIT_TEST_SUITE(TVectorSpillerAdapterTest_MultipleVectors) {

    template <typename T>
    void ManyDifferentSizes_TestRunner() {
        std::vector<std::vector<T, TMKQLAllocator<T>>> vectors;
        
        for (int vectorSize = 0; vectorSize <= 100; ++vectorSize) {
            vectors.push_back(CreateSimpleVectorOfSize<T>(vectorSize));
        }

        SaveRestoreAndCompareVectors<T>(vectors, 20);
    }

    Y_UNIT_TEST(ManyDifferentSizes) {
        TScopedAlloc Alloc(__LOCATION__);
        ManyDifferentSizes_TestRunner<int>();
        ManyDifferentSizes_TestRunner<char>();
    }

    template <typename T>
    void ManyDifferentSizesReversed_TestRunner() {
        std::vector<std::vector<T, TMKQLAllocator<T>>> vectors;
        
        for (int vectorSize = 100; vectorSize >= 0; --vectorSize) {
            vectors.push_back(CreateSimpleVectorOfSize<T>(vectorSize));
        }

        SaveRestoreAndCompareVectors<T>(vectors, 20);
    }

    Y_UNIT_TEST(ManyDifferentSizesReversed) {
        TScopedAlloc Alloc(__LOCATION__);
        ManyDifferentSizesReversed_TestRunner<int>();
        ManyDifferentSizesReversed_TestRunner<char>();
    }

    template <typename T>
    void VectorsInOneChunk_TestRunner() {
        std::vector<std::vector<T, TMKQLAllocator<T>>> vectors;
        
        size_t totalSize = 0;

        for (int vectorSize = 1; vectorSize < 5; ++vectorSize) {
            std::vector v = CreateSimpleVectorOfSize<T>(vectorSize);
            totalSize += vectorSize;
            vectors.push_back(v);
        }

        SaveRestoreAndCompareVectors<T>(vectors, totalSize * sizeof(int) + 10);
    }

    Y_UNIT_TEST(VectorsInOneChunk) {
        TScopedAlloc Alloc(__LOCATION__);
        VectorsInOneChunk_TestRunner<int>();
        VectorsInOneChunk_TestRunner<char>();
    }

    template <typename T>
    void EmptyVectorsInTheMiddle_TestRunner() {
        std::vector<std::vector<T,TMKQLAllocator<T>>> vectors;
        
        size_t totalSize = 0;

        for (int vectorSize = 1; vectorSize < 5; ++vectorSize) {
            std::vector v = CreateSimpleVectorOfSize<T>(vectorSize);
            totalSize += vectorSize;
            vectors.push_back(v);
        }
        vectors.push_back({});
        vectors.push_back({});

        for (int vectorSize = 1; vectorSize < 5; ++vectorSize) {
            std::vector v = CreateSimpleVectorOfSize<T>(vectorSize);
            totalSize += vectorSize;
            vectors.push_back(v);
        }

        SaveRestoreAndCompareVectors<T>(vectors, totalSize * sizeof(T) + 10);
    }

    Y_UNIT_TEST(EmptyVectorsInTheMiddle) {
        TScopedAlloc Alloc(__LOCATION__);
        EmptyVectorsInTheMiddle_TestRunner<int>();
        EmptyVectorsInTheMiddle_TestRunner<char>();
    }

    template <typename T>
    void RequestedVectorPartlyInMemory_TestRunner() {
        std::vector<std::vector<T, TMKQLAllocator<T>>> vectors;
        std::vector<T, TMKQLAllocator<T>> small = CreateSimpleVectorOfSize<T>(1);
        std::vector<T, TMKQLAllocator<T>> big = CreateSimpleVectorOfSize<T>(10);

        vectors.push_back(small);
        vectors.push_back(big);

        // small vector will also load most of big vector to memory
        size_t chunkSizeBytes = (big.size() - small.size()) * sizeof(T);

        SaveRestoreAndCompareVectors<T>(vectors, chunkSizeBytes);
    }

    Y_UNIT_TEST(RequestedVectorPartlyInMemory) {
        TScopedAlloc Alloc(__LOCATION__);
        RequestedVectorPartlyInMemory_TestRunner<int>();
        RequestedVectorPartlyInMemory_TestRunner<char>();
    }

}

} //namespace namespace NKikimr::NMiniKQL
