#include "compact_hash.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/bitmap.h>
#include <util/generic/ylimits.h>
#include <util/generic/hash_set.h>
#include <util/generic/maybe.h>
#include <util/generic/xrange.h>
#include <util/random/shuffle.h>

using namespace NKikimr;
using namespace NKikimr::NCHash;

Y_UNIT_TEST_SUITE(TCompactHashTest) {

    template <typename TItem>
    void TestListPoolPagesImpl(size_t listSize, ui16 countOfLists, size_t expectedListCapacity, ui32 expectedMark) {
        using TPool = TListPool<TItem>;
        TAlignedPagePool pagePool(__LOCATION__);
        TPool pool(pagePool);
        UNIT_ASSERT(countOfLists > 1);

        THashSet<TItem*> lists;

        void* pageAddr = nullptr;
        for (size_t i = 0; i < countOfLists / 2; ++i) {
            TItem* l = pool.template GetList<TItem>(listSize);
            UNIT_ASSERT_VALUES_EQUAL(expectedMark, TPool::GetMark(l));
            UNIT_ASSERT_VALUES_EQUAL(listSize, TListPoolBase::GetPartListSize(l));
            UNIT_ASSERT_VALUES_EQUAL(expectedListCapacity, TListPoolBase::GetListCapacity(l));
            if (0 == i) {
                pageAddr = TAlignedPagePool::GetPageStart(l);
            } else {
                // All lists are from the same page
                UNIT_ASSERT_VALUES_EQUAL(pageAddr, TAlignedPagePool::GetPageStart(l));
            }
            UNIT_ASSERT(lists.insert(l).second);
        }
        // Return all lists except one
        while (lists.size() > 1) {
            auto it = lists.begin();
            pool.ReturnList(*it);
            lists.erase(it);
        }

        for (size_t i = 1; i < countOfLists; ++i) {
            TItem* l = pool.template GetList<TItem>(listSize);
            UNIT_ASSERT_VALUES_EQUAL(expectedMark, TPool::GetMark(l));
            UNIT_ASSERT_VALUES_EQUAL(listSize, TListPoolBase::GetPartListSize(l));
            UNIT_ASSERT_VALUES_EQUAL(expectedListCapacity, TListPoolBase::GetListCapacity(l));
            UNIT_ASSERT_VALUES_EQUAL(pageAddr, TAlignedPagePool::GetPageStart(l));
            UNIT_ASSERT(lists.insert(l).second);
        }

        for (auto l: lists) {
            pool.ReturnList(l);
        }

        THashSet<TItem*> lists2;
        for (size_t i = 0; i < countOfLists; ++i) {
            TItem* l = pool.template GetList<TItem>(listSize);
            // All lists are from the same page
            UNIT_ASSERT_VALUES_EQUAL(pageAddr, TAlignedPagePool::GetPageStart(l));
            UNIT_ASSERT(1 == lists.erase(l));
            UNIT_ASSERT(lists2.insert(l).second);
        }
        TItem* l = pool.template GetList<TItem>(listSize); // New page
        UNIT_ASSERT_VALUES_UNEQUAL(pageAddr, TAlignedPagePool::GetPageStart(l));
        UNIT_ASSERT_VALUES_EQUAL(listSize, TListPoolBase::GetPartListSize(l));
        UNIT_ASSERT_VALUES_EQUAL(expectedListCapacity, TListPoolBase::GetListCapacity(l));
        pool.ReturnList(l);
        for (auto l: lists2) {
            pool.ReturnList(l);
        }
    }

    template <typename TItem>
    void TestListPoolLargeImpl() {
        using TPool = TListPool<TItem>;
        TAlignedPagePool pagePool(__LOCATION__);
        TPool pool(pagePool);
        const size_t listSize = TListPoolBase::GetMaxListSize<TItem>();
        TItem* l = pool.template GetList<TItem>(listSize);
        pool.template IncrementList<TItem>(l);
        UNIT_ASSERT_VALUES_EQUAL((ui32)TListPoolBase::LARGE_MARK, TListPoolBase::GetMark(l));
        UNIT_ASSERT_VALUES_EQUAL(listSize + 1, TListPoolBase::GetPartListSize(l));
        TListPoolBase::SetPartListSize(l, TListPoolBase::GetListCapacity(l));
        pool.template IncrementList<TItem>(l);
        UNIT_ASSERT_VALUES_EQUAL(1, TListPoolBase::GetPartListSize(l));
        UNIT_ASSERT_VALUES_EQUAL(1 + TListPoolBase::GetListCapacity(l), TListPoolBase::GetFullListSize(l));
        pool.ReturnList(l);
    }

    Y_UNIT_TEST(TestListPoolSmallPagesByte) {
        ui16 count = TListPoolBase::GetSmallPageCapacity<ui8>(2);
        TestListPoolPagesImpl<ui8>(2, count, 2, TListPoolBase::SMALL_MARK);
    }

    Y_UNIT_TEST(TestListPoolMediumPagesByte) {
        size_t listSize = TListPoolBase::MAX_SMALL_LIST_SIZE + 1;
        ui16 count = TListPoolBase::GetMediumPageCapacity<ui8>(listSize);
        TestListPoolPagesImpl<ui8>(listSize, count, FastClp2(listSize), TListPoolBase::MEDIUM_MARK);
    }

    Y_UNIT_TEST(TestListPoolLargPagesByte) {
        TestListPoolLargeImpl<ui8>();
    }

    Y_UNIT_TEST(TestListPoolSmallPagesUi64) {
        ui16 count = TListPoolBase::GetSmallPageCapacity<ui64>(2);
        TestListPoolPagesImpl<ui64>(2, count, 2, TListPoolBase::SMALL_MARK);
    }

    Y_UNIT_TEST(TestListPoolMediumPagesUi64) {
        size_t listSize = TListPoolBase::MAX_SMALL_LIST_SIZE + 1;
        ui16 count = TListPoolBase::GetMediumPageCapacity<ui64>(listSize);
        TestListPoolPagesImpl<ui64>(listSize, count, FastClp2(listSize), TListPoolBase::MEDIUM_MARK);
    }

    Y_UNIT_TEST(TestListPoolLargPagesUi64) {
        TestListPoolLargeImpl<ui64>();
    }

    struct TItem {
        ui8 A[256];
    };

    Y_UNIT_TEST(TestListPoolSmallPagesObj) {
        ui16 count = TListPoolBase::GetSmallPageCapacity<TItem>(2);
        TestListPoolPagesImpl<TItem>(2, count, 2, TListPoolBase::SMALL_MARK);
    }

    Y_UNIT_TEST(TestListPoolMediumPagesObj) {
        size_t listSize = TListPoolBase::MAX_SMALL_LIST_SIZE + 1;
        ui16 count = TListPoolBase::GetMediumPageCapacity<TItem>(listSize);
        TestListPoolPagesImpl<TItem>(listSize, count, FastClp2(listSize), TListPoolBase::MEDIUM_MARK);
    }

    Y_UNIT_TEST(TestListPoolLargPagesObj) {
        TestListPoolLargeImpl<TItem>();
    }

    struct TItemHash {
        template <typename T>
        size_t operator() (T num) const {
            return num % 13;
        }
    };

    template <typename TItem>
    void TestHashImpl() {
        const ui32 elementsCount = 32;
        const ui64 sumKeysTarget = elementsCount * (elementsCount - 1) / 2;

        const ui32 addition = 20;
        const ui64 sumValuesTarget = sumKeysTarget + addition * elementsCount;

        TAlignedPagePool pagePool(__LOCATION__);
        TCompactHash<TItem, TItem, TItemHash> hash(pagePool);

        TVector<TItem> elements(elementsCount);
        std::iota(elements.begin(), elements.end(), 0);
        Shuffle(elements.begin(), elements.end());
        for (TItem i: elements) {
            hash.Insert(i, i + addition);
        }

        {
            decltype(hash) hash2(std::move(hash));
            decltype(hash) hash3(pagePool);
            hash3.Swap(hash2);
            hash = hash3;
        }

        for (TItem i: elements) {
            UNIT_ASSERT(hash.Has(i));
            UNIT_ASSERT(hash.Find(i).Ok());
            UNIT_ASSERT_VALUES_EQUAL(i + addition, hash.Find(i).Get().second);
        }

        UNIT_ASSERT(!hash.Has(elementsCount + 1));
        UNIT_ASSERT(!hash.Has(elementsCount + 10));
        UNIT_ASSERT_VALUES_EQUAL(elementsCount, hash.Size());
        UNIT_ASSERT_VALUES_EQUAL(elementsCount, hash.UniqSize());

        ui64 sumKeys = 0;
        ui64 sumValues = 0;
        for (auto it = hash.Iterate(); it.Ok(); ++it) {
            UNIT_ASSERT_VALUES_EQUAL(it.Get().first + addition, it.Get().second);

            sumKeys += it.Get().first;
            sumValues += it.Get().second;
        }
        UNIT_ASSERT_VALUES_EQUAL(sumKeys, sumKeysTarget);
        UNIT_ASSERT_VALUES_EQUAL(sumValues, sumValuesTarget);
    }

    template <typename TItem>
    void TestMultiHashImpl() {
        const ui32 keysCount = 10;
        const ui64 elementsCount = keysCount * (keysCount + 1) / 2;

        TAlignedPagePool pagePool(__LOCATION__);
        TCompactMultiHash<TItem, TItem, TItemHash> hash(pagePool);

        TVector<TItem> keys(keysCount);
        std::iota(keys.begin(), keys.end(), 0);
        Shuffle(keys.begin(), keys.end());

        ui64 sumKeysTarget = 0;
        ui64 sumValuesTarget = 0;
        for (TItem k: keys) {
            sumKeysTarget += k;
            for (TItem i = 0; i < k + 1; ++i) {
                hash.Insert(k, i);
                sumValuesTarget += i;
            }
        }

        {
            decltype(hash) hash2(std::move(hash));
            decltype(hash) hash3(pagePool);
            hash3.Swap(hash2);
            hash = hash3;
        }

        for (TItem k: keys) {
            UNIT_ASSERT(hash.Has(k));
            UNIT_ASSERT_VALUES_EQUAL(k + 1, hash.Count(k));
            auto it = hash.Find(k);
            UNIT_ASSERT(it.Ok());
            TDynBitMap set;
            for (; it.Ok(); ++it) {
                UNIT_ASSERT_VALUES_EQUAL(k, it.GetKey());
                UNIT_ASSERT(!set.Test(it.GetValue()));
                set.Set(it.GetValue());
            }
            UNIT_ASSERT_VALUES_EQUAL(set.Count(), k + 1);
        }

        UNIT_ASSERT(!hash.Has(keysCount + 1));
        UNIT_ASSERT(!hash.Has(keysCount + 10));
        UNIT_ASSERT(!hash.Find(keysCount + 1).Ok());

        UNIT_ASSERT_VALUES_EQUAL(elementsCount, hash.Size());
        UNIT_ASSERT_VALUES_EQUAL(keysCount, hash.UniqSize());

        ui64 sumKeys = 0;
        ui64 sumValues = 0;
        TMaybe<TItem> prevKey;
        for (auto it = hash.Iterate(); it.Ok(); ++it) {
            const auto key = it.GetKey();
            if (prevKey != key) {
                sumKeys += key;
                for (auto valIt = it.MakeCurrentKeyIter(); valIt.Ok(); ++valIt) {
                    UNIT_ASSERT_VALUES_EQUAL(key, valIt.GetKey());
                    sumValues += valIt.GetValue();
                }
                prevKey = key;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(sumKeys, sumKeysTarget);
        UNIT_ASSERT_VALUES_EQUAL(sumValues, sumValuesTarget);

        // Test large lists
        TItem val = 0;
        for (size_t i = 0; i < TListPoolBase::GetLargePageCapacity<TItem>(); ++i) {
            hash.Insert(keysCount, val++);
        }

        TItem check = 0;
        for (auto i = hash.Find(keysCount); i.Ok(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(keysCount, i.GetKey());
            UNIT_ASSERT_VALUES_EQUAL(check++, i.GetValue());
        }
        UNIT_ASSERT_VALUES_EQUAL(check, val);
        UNIT_ASSERT_VALUES_EQUAL(TListPoolBase::GetLargePageCapacity<TItem>(), hash.Count(keysCount));

        for (size_t i = 0; i < TListPoolBase::GetLargePageCapacity<TItem>() + 1; ++i) {
            hash.Insert(keysCount, val++);
        }

        {
            decltype(hash) hash2(std::move(hash));
            decltype(hash) hash3(pagePool);
            hash3.Swap(hash2);
            hash = hash3;
        }

        check = 0;
        for (auto i = hash.Find(keysCount); i.Ok(); ++i) {
            UNIT_ASSERT_VALUES_EQUAL(keysCount, i.GetKey());
            UNIT_ASSERT_VALUES_EQUAL(check++, i.GetValue());
        }
        UNIT_ASSERT_VALUES_EQUAL(check, val);
        UNIT_ASSERT_VALUES_EQUAL(2 * TListPoolBase::GetLargePageCapacity<TItem>() + 1, hash.Count(keysCount));
    }

    template <typename TItem>
    void TestSetImpl() {
        const ui32 elementsCount = 32;
        const ui64 sumKeysTarget = elementsCount * (elementsCount - 1) / 2;

        TAlignedPagePool pagePool(__LOCATION__);
        TCompactHashSet<TItem, TItemHash> hash(pagePool);

        TVector<TItem> elements(elementsCount);
        std::iota(elements.begin(), elements.end(), 0);
        Shuffle(elements.begin(), elements.end());
        for (TItem i: elements) {
            hash.Insert(i);
        }

        {
            decltype(hash) hash2(std::move(hash));
            decltype(hash) hash3(pagePool);
            hash3.Swap(hash2);
            hash = hash3;
        }

        for (TItem i: elements) {
            UNIT_ASSERT(hash.Has(i));
        }

        UNIT_ASSERT(!hash.Has(elementsCount + 1));
        UNIT_ASSERT(!hash.Has(elementsCount + 10));
        UNIT_ASSERT_VALUES_EQUAL(elementsCount, hash.Size());
        UNIT_ASSERT_VALUES_EQUAL(elementsCount, hash.UniqSize());

        ui64 sumKeys = 0;
        for (auto i = hash.Iterate(); i.Ok(); ++i) {
            sumKeys += *i;
        }
        UNIT_ASSERT_VALUES_EQUAL(sumKeys, sumKeysTarget);
    }

    Y_UNIT_TEST(TestHashByte) {
        TestHashImpl<ui8>();
    }

    Y_UNIT_TEST(TestMultiHashByte) {
        TestMultiHashImpl<ui8>();
    }

    Y_UNIT_TEST(TestSetByte) {
        TestSetImpl<ui8>();
    }

    Y_UNIT_TEST(TestHashUi16) {
        TestHashImpl<ui16>();
    }

    Y_UNIT_TEST(TestMultiHashUi16) {
        TestMultiHashImpl<ui16>();
    }

    Y_UNIT_TEST(TestSetUi16) {
        TestSetImpl<ui16>();
    }

    Y_UNIT_TEST(TestHashUi64) {
        TestHashImpl<ui64>();
    }

    Y_UNIT_TEST(TestMultiHashUi64) {
        TestMultiHashImpl<ui64>();
    }

    Y_UNIT_TEST(TestSetUi64) {
        TestSetImpl<ui64>();
    }

    struct TStressHash {
        TStressHash(size_t param)
            : Param(param)
        {
        }

        template <typename T>
        size_t operator() (T num) const {
            return num % Param;
        }
        const size_t Param;
    };

    Y_UNIT_TEST(TestStressSmallLists) {
        TAlignedPagePool pagePool(__LOCATION__);
        for (size_t listSize: xrange<size_t>(2, 17, 1)) {
            const size_t backets = TListPoolBase::GetSmallPageCapacity<ui64>(listSize);
            const size_t elementsCount = backets * listSize;

            for (size_t count: xrange<size_t>(1, elementsCount + 1, elementsCount / 16)) {
                TCompactHashSet<ui64, TStressHash> hash(pagePool, elementsCount, TStressHash(backets));
                for (auto i: xrange(count)) {
                    hash.Insert(i);
                }
                UNIT_ASSERT_VALUES_EQUAL(count, hash.Size());
                UNIT_ASSERT_VALUES_EQUAL(count, hash.UniqSize());
                //hash.PrintStat(Cerr);
            }
        }
    }
}
