#include <ydb/services/persqueue_v1/actors/fill_batched_data_offset.h>

#include <ydb/core/protos/msgbus_pq.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NGRpcProxy::V1;

Y_UNIT_TEST_SUITE(FillBatchedDataOffset) {

// Unit on fill_batched_data_offset.h (FillBatchedData ENSURE helpers).
// Mid-batch / LMC>1 may return resultOffset < ReadOffset; supportive header leak must not Cover.
Y_UNIT_TEST(BatchedResultCoversAndAdvancesReadOffset) {
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffset = parentKeyOffset + 2;
    constexpr ui64 supportiveLeakOffset = 391;

    UNIT_ASSERT_VALUES_EQUAL(BatchedResultMessageCount(0), 1u);
    UNIT_ASSERT_VALUES_EQUAL(BatchedResultMessageCount(1), 1u);
    UNIT_ASSERT_VALUES_EQUAL(BatchedResultMessageCount(5), 5u);

    // LMC=1: exact match at ReadOffset covers.
    UNIT_ASSERT(BatchedResultCoversReadOffset(parentKeyOffset + 2, /*lmc=*/1, readOffset));
    UNIT_ASSERT(BatchedResultCoversReadOffset(parentKeyOffset + 2, /*lmc=*/0, readOffset));

    // LMC=5: tablet rewinds to batch base below ReadOffset; still covers mid-batch.
    UNIT_ASSERT(BatchedResultCoversReadOffset(parentKeyOffset, /*lmc=*/5, readOffset));
    UNIT_ASSERT(parentKeyOffset < readOffset);
    UNIT_ASSERT(BatchedResultCoversReadOffset(parentKeyOffset, /*lmc=*/5, parentKeyOffset + 4));
    UNIT_ASSERT(!BatchedResultCoversReadOffset(parentKeyOffset, /*lmc=*/5, parentKeyOffset + 5));

    // Supportive header leak (production bug: 391 vs ReadOffset ~547189849) → !Covers → AFL_ENSURE.
    UNIT_ASSERT(!BatchedResultCoversReadOffset(supportiveLeakOffset, /*lmc=*/1, readOffset));
    UNIT_ASSERT(!BatchedResultCoversReadOffset(supportiveLeakOffset + 2, /*lmc=*/1, readOffset));

    ui64 advanced = readOffset;
    AdvanceReadOffsetFromBatchedResult(parentKeyOffset + 2, /*lmc=*/1, advanced);
    UNIT_ASSERT_VALUES_EQUAL(advanced, parentKeyOffset + 3);

    advanced = readOffset;
    AdvanceReadOffsetFromBatchedResult(parentKeyOffset, /*lmc=*/5, advanced);
    UNIT_ASSERT_VALUES_EQUAL(advanced, parentKeyOffset + 5);

    advanced = 0;
    AdvanceReadOffsetFromBatchedResult(parentKeyOffset, /*lmc=*/0, advanced);
    UNIT_ASSERT_VALUES_EQUAL(advanced, parentKeyOffset + 1);
}

// B2 minimal: same coverage loop as FillBatchedData over CmdReadResult with parent-space
// offsets after tx key rename mid-blob read. Supportive leak would fail Covers.
Y_UNIT_TEST(FillBatchedDataLoopAfterTxKeyRenameMidRead) {
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffsetStart = parentKeyOffset + 2; // mid-blob in parent/key space
    constexpr ui32 messageCount = 4;

    NKikimrClient::TCmdReadResult res;
    // Tablet returns remaining single-part rows (LMC=1) with absolute parent offsets.
    for (ui32 i = 2; i < messageCount; ++i) {
        auto* r = res.AddResult();
        r->SetOffset(parentKeyOffset + i);
        r->SetLogicalMessageCount(1);
        r->SetPartNo(0);
        r->SetSeqNo(i + 1);
        r->SetWriteTimestampMS(1);
        r->SetData("x");
    }

    ui64 readOffset = readOffsetStart;
    for (ui32 i = 0; i < res.ResultSize(); ++i) {
        const auto& r = res.GetResult(i);
        UNIT_ASSERT_C(
            BatchedResultCoversReadOffset(r.GetOffset(), r.GetLogicalMessageCount(), readOffset),
            "resultOffset=" << r.GetOffset()
                << " lmc=" << r.GetLogicalMessageCount()
                << " readOffset=" << readOffset
                << " (supportive leak would be ~391)");
        AdvanceReadOffsetFromBatchedResult(r.GetOffset(), r.GetLogicalMessageCount(), readOffset);
    }
    UNIT_ASSERT_VALUES_EQUAL(readOffset, parentKeyOffset + messageCount);

    // Negative: if CmdReadResult leaked header-space offsets, Covers fails (session would die).
    NKikimrClient::TCmdReadResult leak;
    auto* bad = leak.AddResult();
    bad->SetOffset(393); // supportive header for mid-read
    bad->SetLogicalMessageCount(1);
    UNIT_ASSERT(!BatchedResultCoversReadOffset(
        bad->GetOffset(), bad->GetLogicalMessageCount(), readOffsetStart));
}

} // Y_UNIT_TEST_SUITE(FillBatchedDataOffset)
