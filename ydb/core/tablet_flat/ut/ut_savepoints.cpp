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

    Y_UNIT_TEST(Undo) {
        TSavepointSeqNumRanges ranges;
        ranges.Add(5, 7);
        ranges.Add(10, 12);
        ranges.Add(20, 20);

        TVector<TString> states;
        TVector<TSavepointSeqNumRanges::TAddUndo> undos;
        auto add = [&](ui32 from, ui32 to) {
            states.push_back(ToString(ranges));
            ranges.Add(from, to, &undos.emplace_back());
        };

        add(30, 31);  // appended at the end
        add(1, 2);    // inserted at the beginning
        add(8, 9);    // merges two ranges through adjacency
        add(6, 11);   // already covered, nothing changes
        add(13, 25);  // merges several ranges
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [1, 2], [5, 25], [30, 31] }");

        // Only the replaced ranges are kept for undo, not a copy of the whole set
        UNIT_ASSERT(!undos[3].Changed);
        UNIT_ASSERT_VALUES_EQUAL(undos[4].Replaced.size(), 2u);

        // Undos applied in reverse order restore every intermediate state
        while (!undos.empty()) {
            ranges.Undo(undos.back());
            undos.pop_back();
            UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), states.back());
            states.pop_back();
        }
        UNIT_ASSERT_VALUES_EQUAL(ToString(ranges), "{ [5, 7], [10, 12], [20, 20] }");
    }

    Y_UNIT_TEST(InvalidRange) {
        TSavepointSeqNumRanges ranges;
        UNIT_ASSERT_EXCEPTION(ranges.Add(7, 5), yexception);
    }

}

Y_UNIT_TEST_SUITE(TTxStatusPageSavepoints) {

    Y_UNIT_TEST(NoRemovedOpsKeepsVersion0) {
        NPage::TTxStatusBuilder builder;
        builder.AddCommitted(123, TRowVersion(1, 2));
        builder.AddRemoved(234);
        auto data = builder.Finish();

        auto label = NPage::TLabelWrapper().Read(data, NPage::EPage::TxStatus);
        UNIT_ASSERT_VALUES_EQUAL(label.Version, 0u);

        NPage::TTxStatusPage page(data);
        UNIT_ASSERT_VALUES_EQUAL(page.GetCommittedItems().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedItems().size(), 1u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedOpsItems().size(), 0u);
    }

    Y_UNIT_TEST(RemovedOpsRoundTrip) {
        TSavepointSeqNumRanges ranges123;
        ranges123.Add(9, 10);
        ranges123.Add(5, 6);
        TSavepointSeqNumRanges ranges345;
        ranges345.Add(1, 1);

        NPage::TTxStatusBuilder builder;
        builder.AddCommitted(123, TRowVersion(1, 2));
        builder.AddRemoved(234);
        builder.AddRemovedOps(345, ranges345);
        builder.AddRemovedOps(123, ranges123);
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
        for (const auto& item : page.GetRemovedOpsItems()) {
            items << item.GetTxId() << ":[" << item.GetFrom() << ", " << item.GetTo() << "] ";
        }
        UNIT_ASSERT_VALUES_EQUAL(TString(items), "123:[5, 6] 123:[9, 10] 345:[1, 1] ");
    }

    // Builds a version 1 page by hand, since the builder always sorts and validates items
    TSharedData MakeRemovedOpsPage(const TVector<NPage::TTxStatusPage::TRemovedOpsItem>& items) {
        using TPage = NPage::TTxStatusPage;

        const size_t size = sizeof(NPage::TLabel) + sizeof(TPage::THeader)
            + sizeof(TPage::TRemovedOpsHeader) + sizeof(TPage::TRemovedOpsItem) * items.size();
        TVector<char> raw(size);
        char* ptr = raw.data();

        WriteUnaligned<NPage::TLabel>(ptr, NPage::TLabel::Encode(NPage::EPage::TxStatus, 1, size));
        ptr += sizeof(NPage::TLabel);
        WriteUnaligned<TPage::THeader>(ptr, TPage::THeader{ 0, 0 });
        ptr += sizeof(TPage::THeader);
        WriteUnaligned<TPage::TRemovedOpsHeader>(ptr, TPage::TRemovedOpsHeader{ ui64(items.size()) });
        ptr += sizeof(TPage::TRemovedOpsHeader);
        memcpy(ptr, items.data(), sizeof(TPage::TRemovedOpsItem) * items.size());

        return TSharedData::Copy(raw.data(), raw.size());
    }

    Y_UNIT_TEST(UnsortedRemovedOpsRejected) {
        using TPage = NPage::TTxStatusPage;

        UNIT_ASSERT_EXCEPTION(TPage(MakeRemovedOpsPage({ { 345, 1, 1 }, { 123, 5, 6 } })), yexception);

        // The same items in the right order are accepted
        TPage page(MakeRemovedOpsPage({ { 123, 5, 6 }, { 345, 1, 1 } }));
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedOpsItems().size(), 2u);
    }

    Y_UNIT_TEST(InvalidRemovedOpsRangeRejected) {
        using TPage = NPage::TTxStatusPage;

        // From > To, including the first item
        UNIT_ASSERT_EXCEPTION(TPage(MakeRemovedOpsPage({ { 123, 6, 5 } })), yexception);
        UNIT_ASSERT_EXCEPTION(TPage(MakeRemovedOpsPage({ { 123, 1, 1 }, { 123, 6, 5 } })), yexception);
        // Seq num 0 cannot be removed
        UNIT_ASSERT_EXCEPTION(TPage(MakeRemovedOpsPage({ { 123, 0, 5 } })), yexception);
        // Ranges of a transaction must not overlap or touch, ranges of different ones may
        UNIT_ASSERT_EXCEPTION(TPage(MakeRemovedOpsPage({ { 123, 1, 5 }, { 123, 3, 7 } })), yexception);
        UNIT_ASSERT_EXCEPTION(TPage(MakeRemovedOpsPage({ { 123, 1, 5 }, { 123, 6, 7 } })), yexception);
        TPage page(MakeRemovedOpsPage({ { 123, 1, 5 }, { 123, 7, 7 }, { 345, 1, 5 } }));
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedOpsItems().size(), 3u);
    }

    Y_UNIT_TEST(DuplicateRemovedOpsTxRejected) {
        TSavepointSeqNumRanges ranges;
        ranges.Add(3, 4);

        NPage::TTxStatusBuilder builder;
        builder.AddRemovedOps(123, ranges);
        UNIT_ASSERT_EXCEPTION(builder.AddRemovedOps(123, ranges), yexception);
    }

    Y_UNIT_TEST(OnlyRemovedOps) {
        TSavepointSeqNumRanges ranges;
        ranges.Add(3, 4);

        NPage::TTxStatusBuilder builder;
        UNIT_ASSERT(!builder);
        builder.AddRemovedOps(123, ranges);
        UNIT_ASSERT(builder);
        auto data = builder.Finish();
        UNIT_ASSERT(data);

        NPage::TTxStatusPage page(data);
        UNIT_ASSERT_VALUES_EQUAL(page.GetCommittedItems().size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedItems().size(), 0u);
        UNIT_ASSERT_VALUES_EQUAL(page.GetRemovedOpsItems().size(), 1u);
    }

}

}
}
