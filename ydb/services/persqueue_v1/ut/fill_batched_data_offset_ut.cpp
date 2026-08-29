#include <ydb/services/persqueue_v1/actors/fill_batched_data.h>
#include <ydb/services/persqueue_v1/actors/fill_batched_data_offset.h>
#include <ydb/services/persqueue_v1/actors/partition_id.h>

#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/msgbus_pq.pb.h>
#include <ydb/core/persqueue/public/nameresolver/nameresolver.h>
#include <ydb/public/api/protos/draft/persqueue_common.pb.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NKikimr::NGRpcProxy::V1;

namespace {

TString MakeRegularDataChunk(const TString& payload) {
    NKikimrPQClient::TDataChunk chunk;
    chunk.SetChunkType(NKikimrPQClient::TDataChunk::REGULAR);
    chunk.SetData(payload);
    chunk.SetCodec(NPersQueueCommon::RAW);
    TString out;
    Y_ABORT_UNLESS(chunk.SerializeToString(&out));
    return out;
}

NKikimr::NPQ::NNameResolver::TTopicNamesPtr MakeTestTopicConverter() {
    NKikimr::NPQ::NNameResolver::TTopicNames names;
    names.Valid = true;
    names.Path = "/Root/topic";
    names.ClientsideName = "topic";
    names.InternalName = "/Root/topic";
    return NKikimr::NPQ::NNameResolver::MakeTopicNamesPtr(std::move(names));
}

TPartitionId MakeTestPartitionId(ui64 partition, ui64 assignId) {
    TPartitionId id;
    id.TopicNames = MakeTestTopicConverter();
    id.Partition = partition;
    id.AssignId = assignId;
    return id;
}

} // namespace

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

// Real FillBatchedData (Topic API): parent-space mid-blob CmdReadResult must pass ENSURE
// and emit message offsets in key space. Supportive leak must not Cover (would AFL_ENSURE).
Y_UNIT_TEST(FillBatchedDataTopicApiAfterTxKeyRenameMidRead) {
    constexpr ui64 parentKeyOffset = 547'189'849;
    constexpr ui64 readOffsetStart = parentKeyOffset + 2;
    constexpr ui32 messageCount = 4;

    auto topic = MakeTestTopicConverter();
    UNIT_ASSERT(topic);

    NKikimrClient::TCmdReadResult res;
    for (ui32 i = 2; i < messageCount; ++i) {
        auto* r = res.AddResult();
        r->SetOffset(parentKeyOffset + i);
        r->SetLogicalMessageCount(1);
        r->SetPartNo(0);
        r->SetSeqNo(i + 1);
        r->SetWriteTimestampMS(1000); // same write ts → one Topic batch
        r->SetCreateTimestampMS(1000 + i);
        r->SetUncompressedSize(1);
        r->SetData(MakeRegularDataChunk(TString(1, static_cast<char>('a' + i))));
    }

    // Negative contract before calling FillBatchedData (AFL_ENSURE would abort the process).
    UNIT_ASSERT(!BatchedResultCoversReadOffset(/*resultOffset=*/393, /*lmc=*/1, readOffsetStart));

    Ydb::Topic::StreamReadMessage::ReadResponse response;
    ui64 readOffset = readOffsetStart;
    ui64 wTime = 0;
    const auto partition = MakeTestPartitionId(/*partition=*/0, /*assignId=*/7);

    const bool hasData = FillBatchedData(
        &response, res, partition, /*readIdToResponse=*/1, readOffset, wTime,
        /*endOffset=*/parentKeyOffset + messageCount + 10, topic);

    UNIT_ASSERT(hasData);
    UNIT_ASSERT_VALUES_EQUAL(readOffset, parentKeyOffset + messageCount);
    UNIT_ASSERT_VALUES_EQUAL(response.partition_data_size(), 1);
    const auto& part = response.partition_data(0);
    UNIT_ASSERT_VALUES_EQUAL(part.partition_session_id(), 7u);

    TVector<ui64> offsets;
    for (const auto& batch : part.batches()) {
        for (const auto& msg : batch.message_data()) {
            offsets.push_back(msg.offset());
        }
    }
    UNIT_ASSERT_VALUES_EQUAL(offsets.size(), messageCount - 2);
    for (size_t i = 0; i < offsets.size(); ++i) {
        UNIT_ASSERT_VALUES_EQUAL(offsets[i], parentKeyOffset + 2 + i);
    }
}

} // Y_UNIT_TEST_SUITE(FillBatchedDataOffset)
