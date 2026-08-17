#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/pqtablet/common/event_helpers.h>

#include <library/cpp/testing/unittest/registar.h>

#ifndef UNIT_ASSERT_TRUE
#define UNIT_ASSERT_TRUE(e)  UNIT_ASSERT((e))
#endif

#ifndef UNIT_ASSERT_FALSE
#define UNIT_ASSERT_FALSE(e) UNIT_ASSERT(!(e))
#endif

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(EventsTest) {

void AddReadOperation(NKikimrPQ::TDataTransaction& tx)
{
    auto* op = tx.AddOperations();
    op->SetCommitOffsetsBegin(0);
    op->SetCommitOffsetsEnd(0);
}

void AddKafkaReadOperation(NKikimrPQ::TDataTransaction& tx)
{
    auto* op = tx.AddOperations();
    op->SetKafkaTransaction(true);
    op->SetCommitOffsetsBegin(0);
    op->SetCommitOffsetsEnd(0);
}

void AddWriteOperation(NKikimrPQ::TDataTransaction& tx, bool skipConflictCheck)
{
    auto* op = tx.AddOperations();
    op->SetSkipConflictCheck(skipConflictCheck);
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_NonDataTx)
{
    TEvPersQueue::TEvProposeTransactionBuilder event;

    event.Record.MutableConfig();

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataEmptyOps)
{
    TEvPersQueue::TEvProposeTransactionBuilder event;

    event.Record.MutableData();

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataOnlyReads)
{
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    AddReadOperation(*tx);
    AddKafkaReadOperation(*tx);

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataAllWritesTrue)
{
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    AddWriteOperation(*tx, true);
    AddWriteOperation(*tx, true);

    UNIT_ASSERT_TRUE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataAllWritesFalse)
{
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    AddWriteOperation(*tx, false);
    AddWriteOperation(*tx, false);

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataMixedFlags)
{
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    AddWriteOperation(*tx, false);
    AddWriteOperation(*tx, true);

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

void AddReadOperation(TVector<NKikimrPQ::TPartitionOperation>& ops)
{
    NKikimrPQ::TPartitionOperation op;
    op.SetCommitOffsetsBegin(0);
    op.SetCommitOffsetsEnd(0);
    ops.push_back(std::move(op));
}

void AddWriteOperation(TVector<NKikimrPQ::TPartitionOperation>& ops, bool skipConflictCheck)
{
    NKikimrPQ::TPartitionOperation op;
    op.SetSkipConflictCheck(skipConflictCheck);

    ops.push_back(std::move(op));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_Empty)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_OnlyReads)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddReadOperation(ops);

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_OneWriteTrue)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddWriteOperation(ops, true);

    UNIT_ASSERT_TRUE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_MultipleWritesTrue)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddWriteOperation(ops, true);
    AddWriteOperation(ops, true);

    UNIT_ASSERT_TRUE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_OneWriteFalse)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddWriteOperation(ops, false);

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_MultipleWritesFalse)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddWriteOperation(ops, false);
    AddWriteOperation(ops, false);

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_MixedFlagsOneEach)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddWriteOperation(ops, false);
    AddWriteOperation(ops, true);

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_ReadsAndWritesTrue)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddReadOperation(ops);
    AddWriteOperation(ops, true);

    UNIT_ASSERT_TRUE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_ReadsAndWritesFalse)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddReadOperation(ops);
    AddWriteOperation(ops, false);

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

Y_UNIT_TEST(AllExistingWritesSkipConflictCheck_ReadsAndWritesMixed)
{
    TVector<NKikimrPQ::TPartitionOperation> ops;

    AddReadOperation(ops);
    AddWriteOperation(ops, true);
    AddWriteOperation(ops, false);

    UNIT_ASSERT_FALSE(AllExistingWritesSkipConflictCheck(ops));
}

}

}
