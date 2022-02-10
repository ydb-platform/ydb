#include <ydb/core/blobstorage/incrhuge/incrhuge_id_dict.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NIncrHuge;

Y_UNIT_TEST_SUITE(TIncrHugeBlobIdDict) {
    Y_UNIT_TEST(Basic) {
        TVector<std::pair<TIncrHugeBlobId, ui8>> values;
        auto callback = [&](TIncrHugeBlobId id, ui8 value) {
            values.emplace_back(id, value);
        };

        TIdLookupTable<ui8, ui8, 2> dict;
        UNIT_ASSERT_VALUES_EQUAL(0, dict.GetNumPagesUsed());
        TIncrHugeBlobId id1 = dict.Create(1);
        UNIT_ASSERT_VALUES_EQUAL(1, dict.GetNumPagesUsed());
        TIncrHugeBlobId id2 = dict.Create(1);
        UNIT_ASSERT_VALUES_EQUAL(1, dict.GetNumPagesUsed());
        TIncrHugeBlobId id3 = dict.Create(1);
        UNIT_ASSERT_VALUES_EQUAL(2, dict.GetNumPagesUsed());
        UNIT_ASSERT_VALUES_EQUAL(1, dict.Lookup(id1));
        UNIT_ASSERT_VALUES_EQUAL(1, dict.Lookup(id2));
        UNIT_ASSERT_VALUES_EQUAL(1, dict.Lookup(id3));
        dict.Enumerate(callback);
        std::sort(values.begin(), values.end());
        UNIT_ASSERT_VALUES_EQUAL(values, (decltype(values){{id1, 1}, {id2, 1}, {id3, 1}}));
        values.clear();
        dict.Delete(id1);
        UNIT_ASSERT_VALUES_EQUAL(2, dict.GetNumPagesUsed());
        dict.Enumerate(callback);
        std::sort(values.begin(), values.end());
        UNIT_ASSERT_VALUES_EQUAL(values, (decltype(values){{id2, 1}, {id3, 1}}));
        values.clear();
        dict.Delete(id3);
        UNIT_ASSERT_VALUES_EQUAL(1, dict.GetNumPagesUsed());
        dict.Delete(id2);
        UNIT_ASSERT_VALUES_EQUAL(0, dict.GetNumPagesUsed());
    }
}
