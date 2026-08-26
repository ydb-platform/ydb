#include "partition_writer_cache_actor_fixture.h"

#include <ydb/core/base/tablet_pipe.h>

#include <memory>

namespace NKikimr::NPersQueueTests {

Y_UNIT_TEST_SUITE(TPartitionWriterCacheActorTests) {

Y_UNIT_TEST_F(WriteReplyOrder, TPartitionWriterCacheActorFixture)
{
    TActorId partitionWriterCache = CreatePartitionWriterCacheActor();

    //
    // write operations within two transactions txId-A and txId-B
    //
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-A", .Cookie=1});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-B", .Cookie=2});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-B", .Cookie=3});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-A", .Cookie=4});

    //
    // The answers from the PQ tablet come in a different order
    //
    PQTablet->AppendWriteReply(2);
    PQTablet->AppendWriteReply(3);
    PQTablet->AppendWriteReply(1);
    PQTablet->AppendWriteReply(4);

    //
    // it is expected that the responses will come in the same order as the requests
    //
    ui64 expectCookie[2] = {1, 1};

    while (expectCookie[0] < 5 || expectCookie[1] < 5) {
        TAutoPtr<IEventHandle> handle;
        auto events =
            Ctx->Runtime->GrabEdgeEvents<NPQ::TEvPartitionWriter::TEvWriteAccepted, NPQ::TEvPartitionWriter::TEvWriteResponse>(handle);

        if (auto accepted = get<0>(events); accepted) {
            UNIT_ASSERT_VALUES_EQUAL(accepted->SessionId, "sessionId");
            UNIT_ASSERT((accepted->TxId == "txId-A") || (accepted->TxId == "txId-B"));
            UNIT_ASSERT_VALUES_EQUAL(accepted->Cookie, expectCookie[0]);

            ++expectCookie[0];
        } else if (auto complete = get<1>(events); complete) {
            UNIT_ASSERT_VALUES_EQUAL(complete->SessionId, "sessionId");
            UNIT_ASSERT((complete->TxId == "txId-A") || (complete->TxId == "txId-B"));
            UNIT_ASSERT(complete->IsSuccess());
            UNIT_ASSERT_VALUES_EQUAL(complete->Record.GetPartitionResponse().GetCookie(), expectCookie[1]);

            ++expectCookie[1];
        } else {
            UNIT_ASSERT(false);
        }
    }
}

Y_UNIT_TEST_F(DropOldWriter, TPartitionWriterCacheActorFixture)
{
    TActorId partitionWriterCache = CreatePartitionWriterCacheActor();

    //
    // no more than four actors writers work within one writing session
    //
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-A", .Cookie=1});
    WaitForPartitionWriterOps({.CreateCount=1});

    AdvanceCurrentTime(TDuration::Seconds(1));
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-B", .Cookie=2});
    WaitForPartitionWriterOps({.CreateCount=1});

    //
    // the transaction time of txId-A and txId-B is different
    //

    AdvanceCurrentTime(TDuration::Seconds(1));
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-C", .Cookie=3});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-D", .Cookie=4});
    WaitForPartitionWriterOps({.CreateCount=2});

    //
    // a writer's actor has been created for each transaction
    //
    EnsurePartitionWriterExist({.SessionId="sessionId", .TxId="txId-A"});
    EnsurePartitionWriterExist({.SessionId="sessionId", .TxId="txId-B"});
    EnsurePartitionWriterExist({.SessionId="sessionId", .TxId="txId-C"});
    EnsurePartitionWriterExist({.SessionId="sessionId", .TxId="txId-D"});

    //
    // a write operation within a new transaction should displace the oldest actor of the writer
    //
    AdvanceCurrentTime(TDuration::Seconds(1));
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-E", .Cookie=5});
    WaitForPartitionWriterOps({.CreateCount=1, .DeleteCount=1});
    EnsureNoDisconnected();

    //
    // this is the new actor for the txId-E transaction
    //
    EnsurePartitionWriterExist({.SessionId="sessionId", .TxId="txId-E"});
    EnsurePartitionWriterNotExist({.SessionId="sessionId", .TxId="txId-A"});

    //
    // a new write operation within the txId-A transaction should displace the writer actor
    //
    AdvanceCurrentTime(TDuration::Seconds(1));
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-A", .Cookie=6});
    WaitForPartitionWriterOps({.CreateCount=1, .DeleteCount=1});

    //
    // now this is the writer actor for the txId-B transaction
    //
    EnsurePartitionWriterExist({.SessionId="sessionId", .TxId="txId-A"});
    EnsurePartitionWriterNotExist({.SessionId="sessionId", .TxId="txId-B"});

    AdvanceCurrentTime(TDuration::Seconds(1));

    //
    // we simulate the delay in processing write operations
    //
    PQTablet->AppendWriteReply(7);
    PQTablet->AppendWriteReply(8);
    PQTablet->AppendWriteReply(9);
    PQTablet->AppendWriteReply(10);
    PQTablet->AppendWriteReply(11);

    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-A", .Cookie=7});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-C", .Cookie=8});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-D", .Cookie=9});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-E", .Cookie=10});
    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-B", .Cookie=11});

    //
    // since there are 4 active transactions and an actor cannot be created for the txId-B transaction,
    // the writing session will end with an error
    //
    EnsureWriteSessionClosed(EErrorCode::OverloadError);
}

Y_UNIT_TEST_F(DefaultWriterInitErrorDoesNotSendWriteResponse, TPartitionWriterCacheActorFixture)
{
    PQTablet->FailGetOwnership();
    CreatePartitionWriterCacheActor({.WaitForInitResult = false});

    TAutoPtr<IEventHandle> handle;
    auto events = Ctx->Runtime->GrabEdgeEvents<
        NPQ::TEvPartitionWriter::TEvInitResult,
        NPQ::TEvPartitionWriter::TEvWriteResponse>(handle);

    auto* init = std::get<0>(events);
    UNIT_ASSERT(init);
    UNIT_ASSERT(!init->IsSuccess());
    UNIT_ASSERT(init->SessionId.empty());
    UNIT_ASSERT(init->TxId.empty());

    auto extra = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvWriteResponse>(TDuration::Seconds(1));
    UNIT_ASSERT(!extra);
}

Y_UNIT_TEST_F(TxWriterInitErrorSendsWriteResponse, TPartitionWriterCacheActorFixture)
{
    TActorId partitionWriterCache = CreatePartitionWriterCacheActor();
    PQTablet->FailGetOwnership();

    SendTxWriteRequest(partitionWriterCache, {.SessionId="sessionId", .TxId="txId-A", .Cookie=1});

    TAutoPtr<IEventHandle> handle;
    auto events = Ctx->Runtime->GrabEdgeEvents<
        NPQ::TEvPartitionWriter::TEvInitResult,
        NPQ::TEvPartitionWriter::TEvWriteResponse>(handle);

    UNIT_ASSERT(!std::get<0>(events));
    auto* write = std::get<1>(events);
    UNIT_ASSERT(write);
    UNIT_ASSERT(!write->IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(write->SessionId, "sessionId");
    UNIT_ASSERT_VALUES_EQUAL(write->TxId, "txId-A");

    auto extraInit = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvInitResult>(TDuration::Seconds(1));
    UNIT_ASSERT(!extraInit);
}

Y_UNIT_TEST_F(WriteBeforeInitResultIsHeld, TPartitionWriterCacheActorFixture)
{
    PQTablet->DelayGetOwnership();
    TActorId partitionWriterCache = CreatePartitionWriterCacheActor({.WaitForInitResult = false});

    SendTxWriteRequest(partitionWriterCache, {.Cookie=1});
    PQTablet->AppendWriteReply(1);

    auto tooEarly = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvInitResult>(TDuration::MilliSeconds(100));
    UNIT_ASSERT(!tooEarly);

    CompleteDelayedGetOwnership();

    auto init = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvInitResult>();
    UNIT_ASSERT(init);
    UNIT_ASSERT(init->IsSuccess());

    auto accepted = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvWriteAccepted>();
    UNIT_ASSERT(accepted);
    UNIT_ASSERT_VALUES_EQUAL(accepted->Cookie, 1);

    auto complete = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvWriteResponse>();
    UNIT_ASSERT(complete);
    UNIT_ASSERT(complete->IsSuccess());
    UNIT_ASSERT_VALUES_EQUAL(complete->Record.GetPartitionResponse().GetCookie(), 1);
}

Y_UNIT_TEST_F(PipeDisconnectForwardsDisconnected, TPartitionWriterCacheActorFixture)
{
    TActorId partitionWriterCache = CreatePartitionWriterCacheActor();

    SendTxWriteRequest(partitionWriterCache, {.Cookie=1});
    WaitForPartitionWriterOps({.CreateCount=1});

    const TActorId writer = TxIdToPartitionWriter.at(MakeTxId("", ""));
    Ctx->Runtime->Send(
        writer,
        Ctx->Edge,
        new TEvTabletPipe::TEvClientDestroyed(PQTabletId, writer, TActorId()),
        0,
        true);

    auto event = Ctx->Runtime->GrabEdgeEvent<NPQ::TEvPartitionWriter::TEvDisconnected>();
    UNIT_ASSERT(event);
    UNIT_ASSERT_EQUAL(event->ErrorCode, EErrorCode::PartitionDisconnected);
}

}

}
