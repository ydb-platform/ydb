#include "double_indexed.h"

#include <library/cpp/testing/unittest/registar.h>

#include <util/generic/string.h>
#include <util/string/vector.h>

namespace NKikimr {

Y_UNIT_TEST_SUITE(DoubleIndexedTests) {
    using TPKey = TString;
    using TSKey = ui32;

    struct TValue {
        TString Payload;

        TValue& Merge(TValue&& x) {
            Payload = JoinStrings(TVector<TString>{Payload, x.Payload}, "");
            return *this;
        }

        bool operator==(const TValue& x) const {
            return Payload == x.Payload;
        }
    };

    struct TMerger {
        TValue& operator()(TValue& dst, TValue&& src) {
            return dst.Merge(std::move(src));
        }
    };

    Y_UNIT_TEST(TestUpsertBySingleKey) {
        TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

        TValue& hello = diMap.Upsert(TPKey("test"), TValue{"hello"});
        UNIT_ASSERT_STRINGS_EQUAL(hello.Payload, "hello");
        UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());

        TValue& world = diMap.Upsert(TSKey(42), TValue{"world"});
        UNIT_ASSERT_STRINGS_EQUAL(world.Payload, "world");
        UNIT_ASSERT_VALUES_EQUAL(2, diMap.Size());
    }

    Y_UNIT_TEST(TestUpsertByBothKeys) {
        TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

        TValue& helloWorld = diMap.Upsert(TPKey("test"), TSKey(42), TValue{"hello, world"});
        UNIT_ASSERT_STRINGS_EQUAL(helloWorld.Payload, "hello, world");
        UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());
    }

    Y_UNIT_TEST(TestMerge) {
        TPKey pKey("test");
        TSKey sKey(42);

        {
            TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

            diMap.Upsert(pKey, TValue{"hello"});
            TValue& helloWorld = diMap.Upsert(pKey, sKey, TValue{", world"});

            UNIT_ASSERT_STRINGS_EQUAL(helloWorld.Payload, "hello, world");
            UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());
        }

        {
            TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

            diMap.Upsert(sKey, TValue{"world"});
            TValue& worldHello = diMap.Upsert(pKey, sKey, TValue{", hello"});

            UNIT_ASSERT_STRINGS_EQUAL(worldHello.Payload, "world, hello");
            UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());
        }

        {
            TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

            diMap.Upsert(pKey, TValue{"hello"});
            UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());

            diMap.Upsert(sKey, TValue{", world"});
            UNIT_ASSERT_VALUES_EQUAL(2, diMap.Size());

            TValue& helloWorld = diMap.Upsert(pKey, sKey, TValue{"!"});
            UNIT_ASSERT_STRINGS_EQUAL(helloWorld.Payload, "hello, world!");
            UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());
        }
    }

    template <typename TKey>
    void TestFindImpl(const TKey& key) {
        TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

        TValue& value = diMap.Upsert(key, TValue{"value"});
        TValue* valuePtr = diMap.FindPtr(key);
        UNIT_ASSERT_UNEQUAL(nullptr, valuePtr);
        UNIT_ASSERT_EQUAL(&value, valuePtr);
    }

    Y_UNIT_TEST(TestFind) {
        TestFindImpl(TPKey("test"));
        TestFindImpl(TSKey(42));
    }

    template <typename TKey>
    void TestEraseImpl(const TKey& key) {
        TDoubleIndexedMap<TPKey, TSKey, TValue, TMerger> diMap;

        diMap.Upsert(key, TValue{"value"});
        UNIT_ASSERT_VALUES_EQUAL(1, diMap.Size());

        diMap.Erase(key);
        UNIT_ASSERT_VALUES_EQUAL(0, diMap.Size());

        TValue* valuePtr = diMap.FindPtr(key);
        UNIT_ASSERT_EQUAL(nullptr, valuePtr);
    }

    Y_UNIT_TEST(TestErase) {
        TestEraseImpl(TPKey("test"));
        TestEraseImpl(TSKey(42));
    }
}

} // NKikimr
