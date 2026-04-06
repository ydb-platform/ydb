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

Y_UNIT_TEST_SUITE(TBitsStorageTests) {
    Y_UNIT_TEST(TestSerializeRestoreFixString) {
        TFixStringBitsStorageConstructor constructor;
        TestSerializeRestore(constructor);
    }

    Y_UNIT_TEST(TestSerializeRestoreBitset) {
        TBitSetStorageConstructor constructor;
        TestSerializeRestore(constructor);
    }
}

}   // namespace NKikimr::NOlap::NIndexes
