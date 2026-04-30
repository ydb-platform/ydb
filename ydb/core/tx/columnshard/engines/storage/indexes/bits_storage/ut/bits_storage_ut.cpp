#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/array_power2.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/bitset.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/bits_storage/fix_string.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/random/fast.h>

#include <unordered_set>

namespace NKikimr::NOlap::NIndexes {

void CheckInsertedValues(const std::unordered_set<ui64>& inserted, ui32 size, const IBitsStorageViewer& storage) {
    for (ui64 i = 0; i < size; ++i) {
        if (inserted.contains(i)) {
            UNIT_ASSERT_C(storage.Get(i), TStringBuilder() << "value " << i << " must be set but is not");
        } else {
            UNIT_ASSERT_C(!storage.Get(i), TStringBuilder() << "value " << i << " must not be set but it is");
        }
    }
}

void TestSerializeRestoreArrayStorage(IBitsStorageConstructor& constructor) {
    constexpr ui32 size = 16384;

    TFastRng64 rng(100500);
    std::unordered_set<ui64> inserted;
    TArrayPower2BitsStorage storage(size);
    for (ui32 i = 0; i < size / 2; ++i) {
        auto item = rng.Uniform(size);
        storage(item);
        inserted.insert(item);
    }
    TString serialized = constructor.SerializeToString(storage);
    auto restoredConclusion = constructor.Restore(serialized);
    UNIT_ASSERT_C(restoredConclusion.IsSuccess(), restoredConclusion.GetErrorMessage());
    auto restored = restoredConclusion.DetachResult();

    CheckInsertedValues(inserted, size, *restored);
}

void TestSerializeRestoreDynBitMap(IBitsStorageConstructor& constructor) {
    constexpr ui32 size = 16384;

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

    CheckInsertedValues(inserted, size, *restored);
}



Y_UNIT_TEST_SUITE(TBitsStorageTests) {
    Y_UNIT_TEST(TestSerializeRestoreArrayStorageToFixString) {
        TFixStringBitsStorageConstructor constructor;
        TestSerializeRestoreArrayStorage(constructor);
    }

    Y_UNIT_TEST(TestSerializeRestoreArrayStorageToBitset) {
        TBitSetStorageConstructor constructor;
        TestSerializeRestoreArrayStorage(constructor);
    }


    Y_UNIT_TEST(TestSerializeRestoreDynBitMapToFixString) {
        TFixStringBitsStorageConstructor constructor;
        TestSerializeRestoreDynBitMap(constructor);
    }

    Y_UNIT_TEST(TestSerializeRestoreDynBitMapToBitset) {
        TBitSetStorageConstructor constructor;
        TestSerializeRestoreDynBitMap(constructor);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
