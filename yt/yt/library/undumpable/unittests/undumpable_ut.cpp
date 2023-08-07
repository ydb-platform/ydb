#include <gtest/gtest.h>

#include <yt/yt/library/undumpable/undumpable.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(UndumpableMemory, Mark)
{
    std::vector<int> bigVector;
    bigVector.resize(1024 * 1024);

    auto mark = MarkUndumpable(&bigVector[0], bigVector.size() * sizeof(bigVector[0]));
    ASSERT_GT(GetUndumpableMemorySize(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);

    CutUndumpableRegionsFromCoredump();

    UnmarkUndumpable(mark);

    ASSERT_EQ(GetUndumpableMemorySize(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);
}

TEST(UndumpableMemory, MarkOOB)
{
    std::vector<int> bigVector;
    bigVector.resize(1024 * 1024);

    MarkUndumpableOob(&bigVector[0], bigVector.size() * sizeof(bigVector[0]));
    ASSERT_GT(GetUndumpableMemorySize(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);

    CutUndumpableRegionsFromCoredump();

    UnmarkUndumpableOob(&bigVector[0]);

    ASSERT_EQ(GetUndumpableMemorySize(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);
}

TEST(UndumpableMemory, UnalignedSize)
{
    std::vector<int> bigVector;
    bigVector.resize(1024 * 1024 + 43);

    auto mark = MarkUndumpable(&bigVector[0], bigVector.size() * sizeof(bigVector[0]));
    ASSERT_GT(GetUndumpableMemorySize(), 0u);
    ASSERT_GT(GetUndumpableMemoryFootprint(), 0u);

    CutUndumpableRegionsFromCoredump();

    UnmarkUndumpable(mark);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
