#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/array_power2.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/bitset.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/fix_string.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>

#include <unordered_set>

namespace NKikimr::NOlap::NIndexes {

void TestSerializeRestore(IBitsStorageConstructor& constructor) {
    ui32 size = 16384;

    TFastRng64 rng(100500);
    std::unordered_set<ui64> inserted;
    TDynBitMap bitmap;
    bitmap.Reserve(size);
    for (ui32 i = 0; i < size / 2; ++i) {
        auto item = rng.Uniform(size);
        bitmap.Set(item);
        inserted.insert(item);
    }
    TString serialized = constructor.SerializeToString(std::move(bitmap));
    auto restoredConclusion = constructor.Restore(serialized);
    UNIT_ASSERT_C(restoredConclusion.IsSuccess(), restoredConclusion.GetErrorMessage());
    auto restored = restoredConclusion.DetachResult();

    for (ui64 i = 0; i < size; ++i) {
        if (inserted.contains(i)) {
            UNIT_ASSERT_C(restored->Get(i), TStringBuilder() << "value " << i << " must be set but is not");
        } else {
            UNIT_ASSERT_C(!restored->Get(i), TStringBuilder() << "value " << i << " must not be set but it is");
        }
    }
}

void RunTest(const std::vector<ui64>& elements, ui32 size) {
    std::unordered_set<ui64> inserted;
    TArrayPower2BitsStorage array(size);

    for (auto i : elements) {
        array(i);
        inserted.insert(i % size);
    }

    auto str = array.SerializeToString();
    TFixStringBitsStorage stringStorage(str);

    for (ui64 i = 0; i < size; ++i) {
        if (inserted.contains(i)) {
            UNIT_ASSERT_C(stringStorage.Get(i), TStringBuilder() << "value " << i << " is not set");
        } else {
            UNIT_ASSERT_C(!stringStorage.Get(i), TStringBuilder() << "value " << i << " is set while it must not");
        }
    }
}


Y_UNIT_TEST_SUITE(TBitsStorageTests) {
    Y_UNIT_TEST(TestSerializeRestoreFixString) {
        TFixStringBitsStorageConstructor constructor;
        TestSerializeRestore(constructor);
    }

    Y_UNIT_TEST(TestSerializeRestoreBitset) {
        TBitSetStorageConstructor constructor;
        TestSerializeRestore(constructor);
    }

    Y_UNIT_TEST(TestCompatibility) {
        ui32 size = 4096;
        RunTest(std::vector<ui64>{0, 1, 100, size - 1, size + 10, size + 100}, size);
    }

    Y_UNIT_TEST(TestCompatibilityRandom) {
        ui32 size = 16384;

        TFastRng64 rng(100500);
        std::vector<ui64> values;
        for (ui32 i = 0; i < size / 2; ++i) {
            values.push_back(rng.Uniform(size * 100));
        }
        RunTest(values, size);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
