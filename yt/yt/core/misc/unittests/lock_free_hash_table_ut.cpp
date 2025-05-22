#include "lock_free_hash_table_and_concurrent_cache_helpers.h"

#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/misc/lock_free_hash_table.h>

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

struct TElement final
{
    ui64 Hash;
    ui32 Size;
    char Data[0];

    static constexpr bool EnableHazard = true;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

template <>
struct THash<NYT::TElement>
{
    size_t operator()(const NYT::TElement* value) const
    {
        return value->Hash;
    }
};

template <>
struct TEqualTo<NYT::TElement>
{
    bool operator()(const NYT::TElement* lhs, const NYT::TElement* rhs) const
    {
        return lhs->Hash == rhs->Hash &&
            lhs->Size == rhs->Size &&
            memcmp(lhs->Data, rhs->Data, lhs->Size) == 0;
    }
};

namespace NYT {
namespace {

////////////////////////////////////////////////////////////////////////////////

TEST(TLockFreeHashTableTest, Simple)
{
    size_t keyColumnCount = 3;
    size_t columnCount = 5;

    TLockFreeHashTable<TElement> table(1000);

    THash<TElement> hash;
    TEqualTo<TElement> equalTo;

    std::vector<TIntrusivePtr<TElement>> checkTable;

    size_t iterations = 1000;

    TRandomCharGenerator randomChar(0);

    for (size_t index = 0; index < iterations; ++index) {
        auto item = NewWithExtraSpace<TElement>(columnCount);
        {
            item->Size = keyColumnCount;
            for (size_t pos = 0; pos < columnCount; ++pos) {
                item->Data[pos] = randomChar();
            }
            item->Hash = THash<TStringBuf>{}(TStringBuf(&item->Data[0], keyColumnCount));
        }
        checkTable.push_back(item);
    }

    std::sort(checkTable.begin(), checkTable.end(), [] (
        const TIntrusivePtr<TElement>& lhs,
        const TIntrusivePtr<TElement>& rhs)
    {
        return memcmp(lhs->Data, rhs->Data, lhs->Size) < 0;
    });

    auto it = std::unique(checkTable.begin(), checkTable.end(), [&] (
        const TIntrusivePtr<TElement>& lhs,
        const TIntrusivePtr<TElement>& rhs)
    {
        return equalTo(lhs.Get(), rhs.Get());
    });

    checkTable.erase(it, checkTable.end());

    std::random_shuffle(checkTable.begin(), checkTable.end());

    for (const auto& item : checkTable) {
        auto fingerprint = hash(item.Get());
        table.Insert(fingerprint, item);
    }

    for (const auto& item : checkTable) {
        auto fingerprint = hash(item.Get());
        auto found = table.Find(fingerprint, item.Get());

        EXPECT_TRUE(found);
    }

    std::vector<TIntrusivePtr<TElement>> updateTable;
    for (size_t index = 0; index < checkTable.size(); index += 2) {
        const auto& current = checkTable[index];

        auto item = NewWithExtraSpace<TElement>(columnCount);
        memcpy(item.Get(), current.Get(), sizeof(TElement) + columnCount);
        std::swap(item->Data[columnCount - 1], item->Data[columnCount - 2]);

        updateTable.push_back(item);
    }

    for (const auto& item : updateTable) {
        auto fingerprint = hash(item.Get());
        auto foundRef = table.FindRef(fingerprint, item.Get());
        EXPECT_TRUE(static_cast<bool>(foundRef));
        foundRef.Replace(item);
    }

    for (const auto& item : checkTable) {
        auto fingerprint = hash(item.Get());
        auto found = table.Find(fingerprint, item.Get());

        EXPECT_TRUE(found);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT

