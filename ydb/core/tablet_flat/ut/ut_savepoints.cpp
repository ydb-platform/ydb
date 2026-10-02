#include <ydb/core/tablet_flat/flat_page_txstatus.h>
#include <ydb/core/tablet_flat/flat_table_savepoints.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {
namespace NTable {

Y_UNIT_TEST_SUITE(TSavepointSeqNumRanges) {

    Y_UNIT_TEST(AddAndContains) {
        TSavepointSeqNumRanges ranges;
        UNIT_ASSERT(ranges.Empty());
        UNIT_ASSERT(!ranges.Contains(1));

        UNIT_ASSERT(ranges.Add(5, 7));
        UNIT_ASSERT(ranges.Add(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [5, 7], [10, 10] }");

        UNIT_ASSERT(!ranges.Contains(4));
        UNIT_ASSERT(ranges.Contains(5));
        UNIT_ASSERT(ranges.Contains(7));
        UNIT_ASSERT(!ranges.Contains(8));
        UNIT_ASSERT(ranges.Contains(10));
        UNIT_ASSERT(!ranges.Contains(11));

        // Already covered ranges don't change the set
        UNIT_ASSERT(!ranges.Add(6, 7));
        UNIT_ASSERT(!ranges.Add(10, 10));
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [5, 7], [10, 10] }");
    }

    Y_UNIT_TEST(Merge) {
        TSavepointSeqNumRanges ranges;
        ranges.Add(5, 7);
        ranges.Add(10, 12);
        ranges.Add(20, 20);

        // Adjacent ranges are merged
        UNIT_ASSERT(ranges.Add(8, 8));
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [5, 8], [10, 12], [20, 20] }");

        // A range spanning several ranges merges all of them
        UNIT_ASSERT(ranges.Add(9, 19));
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [5, 20] }");

        // Ranges before and after
        UNIT_ASSERT(ranges.Add(1, 2));
        UNIT_ASSERT(ranges.Add(Max<ui32>(), Max<ui32>()));
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [1, 2], [5, 20], [4294967295, 4294967295] }");
        UNIT_ASSERT(ranges.Contains(Max<ui32>()));

        TSavepointSeqNumRanges other;
        other.Add(3, 4);
        UNIT_ASSERT(ranges.Add(other));
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [1, 20], [4294967295, 4294967295] }");
        UNIT_ASSERT(!ranges.Add(other));
    }

    Y_UNIT_TEST(InvalidRange) {
        TSavepointSeqNumRanges ranges;
        UNIT_ASSERT_EXCEPTION(ranges.Add(7, 5), yexception);
    }

}

Y_UNIT_TEST_SUITE(TTxStatusPageSavepoints) {

    Y_UNIT_TEST(NoRolledBackKeepsVersion0) {
        NPage::TTxStatusBuilder builder;
        builder.AddCommitted(123, TRowVersion(1, 2));
        builder.AddRemoved(234);
        auto data = builder.Finish();

        auto label = NPage::TLabelWrapper().Read(data, NPage::EPage::TxStatus);
        UNIT_ASSERT_VALUES_EQUAL(label.Version, 0u);

        NPage::TTxStatusPage page(data);
        UNIT_ASSERT_VALUES_EQUAL(page.GetCommittedItems().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedItems().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRolledBackItems().size(), 0u);
    }

    Y_UNIT_TEST(RolledBackRoundTrip) {
        TSavepointSeqNumRanges ranges123;
        ranges123.Add(9, 10);
        ranges123.Add(5, 6);
        TSavepointSeqNumRanges ranges345;
        ranges345.Add(1, 1);

        NPage::TTxStatusBuilder builder;
        builder.AddCommitted(123, TRowVersion(1, 2));
        builder.AddRemoved(234);
        builder.AddRolledBack(345, ranges345);
        builder.AddRolledBack(123, ranges123);
        auto data = builder.Finish();

        auto label = NPage::TLabelWrapper().Read(data, NPage::EPage::TxStatus);
        UNIT_ASSERT_VALUES_EQUAL(label.Version, 1u);

        NPage::TTxStatusPage page(data);
        UNIT_ASSERT_VALUES_EQUAL(page.GetCommittedItems().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetCommittedItems()[0].GetTxId(), 123u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedItems().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedItems()[0].GetTxId(), 234u);

        // Items are sorted by (TxId, From)
        TStringBuilder items;
        for (const auto& item : page.GetRolledBackItems()) {
            items << item.GetTxId() << ":[" << item.GetFrom() << ", " << item.GetTo() << "] ";
        }
        UNIT_ASSERT_VALUES_EQUAL(TString(items), "123:[5, 6] 123:[9, 10] 345:[1, 1] ");
    }

    Y_UNIT_TEST(OnlyRolledBack) {
        TSavepointSeqNumRanges ranges;
        ranges.Add(3, 4);

        NPage::TTxStatusBuilder builder;
        UNIT_ASSERT(!builder);
        builder.AddRolledBack(123, ranges);
        UNIT_ASSERT(builder);
        auto data = builder.Finish();
        UNIT_ASSERT(data);

        NPage::TTxStatusPage page(data);
        UNIT_ASSERT_VALUES_EQUAL(page.GetCommittedItems().size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedItems().size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRolledBackItems().size(), 1u);
    }

}

}
}
