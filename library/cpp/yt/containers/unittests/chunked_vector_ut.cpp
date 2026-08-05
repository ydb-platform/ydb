#include <library/cpp/yt/containers/chunked_vector.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <iterator>
#include <string>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

constexpr int ChunkSize = 4;

using TVector = TChunkedVector<int, ChunkSize>;

////////////////////////////////////////////////////////////////////////////////

TEST(TChunkedVectorTest, Empty)
{
    TVector vector;
    EXPECT_EQ(0, std::ssize(vector));
    EXPECT_TRUE(vector.Empty());
}

TEST(TChunkedVectorTest, PushBackWithinChunk)
{
    TVector vector;
    for (int index = 0; index < 3; ++index) {
        vector.PushBack(index);
        EXPECT_EQ(index + 1, std::ssize(vector));
        EXPECT_FALSE(vector.Empty());
    }

    for (int index = 0; index < 3; ++index) {
        EXPECT_EQ(index, vector[index]);
    }
}

TEST(TChunkedVectorTest, PushBackAcrossChunks)
{
    TVector vector;
    constexpr int Count = 10 * ChunkSize + 1;
    for (int index = 0; index < Count; ++index) {
        vector.PushBack(index);
    }

    EXPECT_EQ(Count, std::ssize(vector));
    for (int index = 0; index < Count; ++index) {
        EXPECT_EQ(index, vector[index]);
    }
}

TEST(TChunkedVectorTest, PopBack)
{
    TVector vector;
    for (int index = 0; index < 3 * ChunkSize; ++index) {
        vector.PushBack(index);
    }

    vector.PopBack();
    EXPECT_EQ(3 * ChunkSize - 1, std::ssize(vector));
    for (int index = 0; index < 3 * ChunkSize - 1; ++index) {
        EXPECT_EQ(index, vector[index]);
    }

    while (!vector.Empty()) {
        vector.PopBack();
    }
    EXPECT_EQ(0, std::ssize(vector));
}

TEST(TChunkedVectorTest, Mutate)
{
    TVector vector;
    for (int index = 0; index < 2 * ChunkSize; ++index) {
        vector.PushBack(index);
    }

    vector[0] = 100;
    vector[ChunkSize] = 200;

    EXPECT_EQ(100, vector[0]);
    EXPECT_EQ(200, vector[ChunkSize]);
    EXPECT_EQ(1, vector[1]);
}

//! Stable addresses are what makes reads from other threads safe.
TEST(TChunkedVectorTest, ElementAddressesAreStable)
{
    TVector vector;
    vector.PushBack(0);
    const int* firstElement = &vector[0];

    for (int index = 1; index < 100 * ChunkSize; ++index) {
        vector.PushBack(index);
    }

    EXPECT_EQ(firstElement, &vector[0]);
    EXPECT_EQ(0, *firstElement);
}

TEST(TChunkedVectorTest, ReserveChunks)
{
    TVector vector;
    vector.ReserveChunks(16);
    EXPECT_EQ(0, std::ssize(vector));

    for (int index = 0; index < 16 * ChunkSize; ++index) {
        vector.PushBack(index);
    }
    EXPECT_EQ(16 * ChunkSize, std::ssize(vector));
}

TEST(TChunkedVectorTest, NonPodElements)
{
    TChunkedVector<std::string, ChunkSize> vector;
    for (int index = 0; index < 3 * ChunkSize; ++index) {
        vector.PushBack(std::to_string(index));
    }

    for (int index = 0; index < 3 * ChunkSize; ++index) {
        EXPECT_EQ(std::to_string(index), vector[index]);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
