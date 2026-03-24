#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/events/internal.h>

#include <library/cpp/testing/unittest/registar.h>

#define UNIT_ASSERT_TRUE(e)  UNIT_ASSERT((e))
#define UNIT_ASSERT_FALSE(e) UNIT_ASSERT(!(e))

namespace NKikimr::NPQ {

Y_UNIT_TEST_SUITE(EventsTest) {

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_NonDataTx) {
    TEvPersQueue::TEvProposeTransactionBuilder event;

    event.Record.MutableConfig();

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataEmptyOps) {
    TEvPersQueue::TEvProposeTransactionBuilder event;

    event.Record.MutableData();

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataOnlyReads) {
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    tx->AddOperations();
    tx->AddOperations();

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataAllWritesTrue) {
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    auto* op = tx->AddOperations();
    op->SetSkipConflictCheck(true);
    op = tx->AddOperations();
    op->SetSkipConflictCheck(true);

    UNIT_ASSERT_TRUE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataAllWritesFalse) {
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    auto* op = tx->AddOperations();
    op->SetSkipConflictCheck(false);
    op = tx->AddOperations();
    op->SetSkipConflictCheck(false);

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

Y_UNIT_TEST(TEvProposeTransaction_GetSkipSrcIdInfo_DataMixedFlags) {
    TEvPersQueue::TEvProposeTransactionBuilder event;

    auto* tx = event.Record.MutableData();
    auto* op = tx->AddOperations();
    op->SetSkipConflictCheck(false);
    op = tx->AddOperations();
    op->SetSkipConflictCheck(true);

    UNIT_ASSERT_FALSE(event.GetSkipSrcIdInfo());
}

}

}
