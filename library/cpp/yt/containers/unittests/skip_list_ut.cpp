#include <library/cpp/yt/containers/skip_list.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <cstdlib>
#include <iterator>
#include <set>
#include <string>
#include <string_view>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TComparer
{
    int operator()(int lhs, int rhs) const
    {
        if (lhs < rhs) {
            return -1;
        }
        if (lhs > rhs) {
            return +1;
        }
        return 0;
    }
};

class TSkipListTest
    : public ::testing::Test
{
public:
    TChunkedMemoryPool Pool;
    TSkipList<int, TComparer> List;

    TSkipListTest()
        : List(&Pool, TComparer{})
    { }
};

TEST_F(TSkipListTest, Empty)
{
    EXPECT_EQ(List.GetSize(), 0);

    EXPECT_FALSE(List.FindEqualTo(1).IsValid());

    EXPECT_FALSE(List.FindGreaterThanOrEqualTo(1).IsValid());
    EXPECT_FALSE(List.FindLessThanOrEqualTo(1).IsValid());
}

TEST_F(TSkipListTest, Singleton)
{
    EXPECT_TRUE(List.Insert(0));
    EXPECT_EQ(List.GetSize(), 1);

    EXPECT_FALSE(List.FindEqualTo(1).IsValid());

    EXPECT_FALSE(List.FindGreaterThanOrEqualTo(1).IsValid());

    {
        auto it = List.FindGreaterThanOrEqualTo(-1);
        EXPECT_TRUE(it.IsValid());
        EXPECT_EQ(it.GetCurrent(), 0);
        it.MoveNext();
        EXPECT_FALSE(it.IsValid());
    }

    {
        auto it = List.FindGreaterThanOrEqualTo(0);
        EXPECT_TRUE(it.IsValid());
        EXPECT_EQ(it.GetCurrent(), 0);
        it.MoveNext();
        EXPECT_FALSE(it.IsValid());
    }
}

TEST_F(TSkipListTest, OneToTen)
{
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(List.Insert(i));
    }
    EXPECT_EQ(List.GetSize(), 10);

    for (int i = 0; i < 10; ++i) {
        auto it = List.FindGreaterThanOrEqualTo(i);
        for (int j = i; j < 10; ++j) {
            EXPECT_TRUE(it.IsValid());
            EXPECT_EQ(it.GetCurrent(), j);
            it.MoveNext();
        }
        EXPECT_FALSE(it.IsValid());
    }

    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(List.FindEqualTo(i).IsValid());
    }

    EXPECT_FALSE(List.FindEqualTo(-1).IsValid());
    EXPECT_FALSE(List.FindEqualTo(11).IsValid());
}

TEST_F(TSkipListTest, TwentyToZeroStepTwo)
{
    for (int i = 20; i > 0; i -= 2) {
        EXPECT_TRUE(List.Insert(i));
    }
    EXPECT_EQ(List.GetSize(), 10);

    for (int i = 3; i < 21; i += 2) {
        auto it = List.FindLessThanOrEqualTo(i);
        for (int j = i - 1; j > 0; j -= 2) {
            EXPECT_TRUE(it.IsValid());
            EXPECT_EQ(it.GetCurrent(), j);
            it.MovePrev();
        }
        EXPECT_FALSE(it.IsValid());
    }

    for (int i = 2; i < 21; i += 2) {
        EXPECT_TRUE(List.FindEqualTo(i).IsValid());
    }

    EXPECT_FALSE(List.FindEqualTo(-1).IsValid());
    EXPECT_FALSE(List.FindEqualTo(21).IsValid());
}

TEST_F(TSkipListTest, Random)
{
    std::srand(42);
    std::set<int> set;
    for (int i = 0; i < 100000; ++i) {
        int value = std::rand() % 1000;
        EXPECT_EQ(List.Insert(value), set.insert(value).second);
    }
    EXPECT_EQ(List.GetSize(), std::ssize(set));

    for (int value : set) {
        EXPECT_TRUE(List.FindEqualTo(value).IsValid());
    }

    auto it = List.FindGreaterThanOrEqualTo(*set.begin());
    for (int value : set) {
        EXPECT_TRUE(it.IsValid());
        EXPECT_EQ(it.GetCurrent(), value);
        it.MoveNext();
    }
}

TEST_F(TSkipListTest, LookupBoundaries)
{
    EXPECT_TRUE(List.Insert(10));
    EXPECT_TRUE(List.Insert(20));

    EXPECT_FALSE(List.FindLessThanOrEqualTo(9).IsValid());
    EXPECT_EQ(10, List.FindLessThanOrEqualTo(10).GetCurrent());
    EXPECT_EQ(10, List.FindLessThanOrEqualTo(15).GetCurrent());
    EXPECT_EQ(20, List.FindLessThanOrEqualTo(20).GetCurrent());
    EXPECT_EQ(20, List.FindLessThanOrEqualTo(21).GetCurrent());

    EXPECT_EQ(10, List.FindGreaterThanOrEqualTo(9).GetCurrent());
    EXPECT_EQ(10, List.FindGreaterThanOrEqualTo(10).GetCurrent());
    EXPECT_EQ(20, List.FindGreaterThanOrEqualTo(15).GetCurrent());
    EXPECT_EQ(20, List.FindGreaterThanOrEqualTo(20).GetCurrent());
    EXPECT_FALSE(List.FindGreaterThanOrEqualTo(21).IsValid());
}

struct TStringComparer
{
    int operator()(std::string_view lhs, std::string_view rhs) const
    {
        return lhs.compare(rhs);
    }
};

TEST(TSkipListCustomInsertTest, ProviderAndExistingKeyConsumer)
{
    TChunkedMemoryPool pool;
    TSkipList<std::string, TStringComparer> list(&pool, TStringComparer{});

    bool providerCalled = false;
    bool consumerCalled = false;

    list.Insert(
        std::string_view("key"),
        [&] {
            providerCalled = true;
            return std::string("key");
        },
        [&] (const std::string&) {
            consumerCalled = true;
        });

    EXPECT_TRUE(providerCalled);
    EXPECT_FALSE(consumerCalled);

    providerCalled = false;
    list.Insert(
        std::string_view("key"),
        [&] {
            providerCalled = true;
            return std::string("other");
        },
        [&] (const std::string& key) {
            consumerCalled = true;
            EXPECT_EQ("key", key);
        });

    EXPECT_FALSE(providerCalled);
    EXPECT_TRUE(consumerCalled);
    EXPECT_EQ(1, list.GetSize());
}

struct TTrackedKey
{
    static inline int AliveCount = 0;

    int Value = 0;

    TTrackedKey()
    {
        ++AliveCount;
    }

    explicit TTrackedKey(int value)
        : Value(value)
    {
        ++AliveCount;
    }

    TTrackedKey(const TTrackedKey& other)
        : Value(other.Value)
    {
        ++AliveCount;
    }

    ~TTrackedKey()
    {
        --AliveCount;
    }
};

struct TTrackedKeyComparer
{
    int operator()(const TTrackedKey& lhs, const TTrackedKey& rhs) const
    {
        return (lhs.Value > rhs.Value) - (lhs.Value < rhs.Value);
    }
};

TEST(TSkipListLifetimeTest, DestroysNonTrivialKeys)
{
    EXPECT_EQ(0, TTrackedKey::AliveCount);

    TChunkedMemoryPool pool;
    {
        TSkipList<TTrackedKey, TTrackedKeyComparer> list(&pool, TTrackedKeyComparer{});
        EXPECT_EQ(1, TTrackedKey::AliveCount);

        EXPECT_TRUE(list.Insert(TTrackedKey(1)));
        EXPECT_TRUE(list.Insert(TTrackedKey(2)));
        EXPECT_FALSE(list.Insert(TTrackedKey(1)));
        EXPECT_EQ(3, TTrackedKey::AliveCount);
    }

    EXPECT_EQ(0, TTrackedKey::AliveCount);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT
