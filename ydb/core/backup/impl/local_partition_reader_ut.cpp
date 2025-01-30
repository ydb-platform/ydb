#include "local_partition_reader.h"

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
// for now just reuse env from replication for simplification
#include <ydb/core/tx/replication/ut_helpers/test_env.h>
#include <ydb/core/tx/replication/service/worker.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NBackup::NImpl {

using namespace NReplication::NTestHelpers;
using namespace NReplication::NService;
using namespace NPersQueue;

Y_UNIT_TEST_SUITE(LocalPartitionReader) {
    constexpr static ui64 INITIAL_OFFSET = 3;
    constexpr static ui64 PARTITION = 7;
    constexpr static const char* PARTITION_STR = "7";
    constexpr static const char* OFFLOAD_ACTOR_NAME = "__OFFLOAD_ACTOR__";

    void GrabInitialPQRequest(TTestActorRuntime& runtime) {
        TAutoPtr<IEventHandle> handle;
        auto getOffset = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvRequest>(handle);
        UNIT_ASSERT(getOffset->Record.HasPartitionRequest());
        UNIT_ASSERT_VALUES_EQUAL(getOffset->Record.GetPartitionRequest().GetPartition(), PARTITION);
        UNIT_ASSERT(getOffset->Record.GetPartitionRequest().HasCmdGetClientOffset());
        auto getOffsetCmd = getOffset->Record.GetPartitionRequest().GetCmdGetClientOffset();
        UNIT_ASSERT_VALUES_EQUAL(getOffsetCmd.GetClientId(), OFFLOAD_ACTOR_NAME);
    }

    void GrabReadPQRequest(TTestActorRuntime& runtime, ui32 expectedOffset) {
        TAutoPtr<IEventHandle> handle;
        auto read = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvRequest>(handle);
        UNIT_ASSERT(read->Record.HasPartitionRequest());
        UNIT_ASSERT_VALUES_EQUAL(read->Record.GetPartitionRequest().GetPartition(), PARTITION);
        UNIT_ASSERT(read->Record.GetPartitionRequest().HasCmdRead());
        auto readCmd = read->Record.GetPartitionRequest().GetCmdRead();
        UNIT_ASSERT_VALUES_EQUAL(readCmd.GetClientId(), OFFLOAD_ACTOR_NAME);
        UNIT_ASSERT_VALUES_EQUAL(readCmd.GetOffset(), expectedOffset);
    }

    void GrabDataEvent(TTestActorRuntime& runtime, ui32 dataPatternCookie) {
        TAutoPtr<IEventHandle> handle;
        auto data = runtime.GrabEdgeEventRethrow<TEvWorker::TEvData>(handle);
        UNIT_ASSERT_VALUES_EQUAL(data->Source, PARTITION_STR);
        UNIT_ASSERT_VALUES_EQUAL(data->Records.size(), 2);
        UNIT_ASSERT_VALUES_EQUAL(data->Records[0].Offset, INITIAL_OFFSET + dataPatternCookie * 2);
        UNIT_ASSERT_VALUES_EQUAL(data->Records[0].Data, Sprintf("1-%d", dataPatternCookie));
        UNIT_ASSERT_VALUES_EQUAL(data->Records[1].Offset, INITIAL_OFFSET + dataPatternCookie * 2 + 1);
        UNIT_ASSERT_VALUES_EQUAL(data->Records[1].Data, Sprintf("2-%d", dataPatternCookie));
    }

    TEvPersQueue::TEvResponse* GenerateData(ui32 dataPatternCookie) {
        auto* readResponse = new TEvPersQueue::TEvResponse;
        readResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        auto& cmdReadResult = *readResponse->Record.MutablePartitionResponse()->MutableCmdReadResult();
        auto& readResult1 = *cmdReadResult.AddResult();
        NKikimrPQClient::TDataChunk msg1;
        msg1.SetData(Sprintf("1-%d", dataPatternCookie));
        TString msg1Str;
        UNIT_ASSERT(msg1.SerializeToString(&msg1Str));
        readResult1.SetOffset(INITIAL_OFFSET + dataPatternCookie * 2);
        readResult1.SetData(msg1Str);
        auto& readResult2 = *cmdReadResult.AddResult();
        NKikimrPQClient::TDataChunk msg2;
        msg2.SetData(Sprintf("2-%d", dataPatternCookie));
        TString msg2Str;
        UNIT_ASSERT(msg2.SerializeToString(&msg2Str));
        readResult2.SetOffset(INITIAL_OFFSET + dataPatternCookie * 2 + 1);
        readResult2.SetData(msg2Str);

        return readResponse;
    }

    TEvPersQueue::TEvResponse* GenerateEmptyData() {
        auto* readResponse = new TEvPersQueue::TEvResponse;
        readResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        readResponse->Record.MutablePartitionResponse()->MutableCmdReadResult();

        return readResponse;
    }

    Y_UNIT_TEST(Simple) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        TActorId pqtablet = runtime.AllocateEdgeActor();
        TActorId reader = runtime.Register(CreateLocalPartitionReader(pqtablet, PARTITION));
        runtime.EnableScheduleForActor(reader);
        TActorId worker = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvHandshake));

        GrabInitialPQRequest(runtime);

        auto* getOffsetResponse = new TEvPersQueue::TEvResponse;
        getOffsetResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        getOffsetResponse->Record.MutablePartitionResponse()->MutableCmdGetClientOffsetResult()->SetOffset(INITIAL_OFFSET);
        runtime.Send(new IEventHandle(reader, pqtablet, getOffsetResponse));

        auto handshake = runtime.GrabEdgeEventRethrow<TEvWorker::TEvHandshake>(handle);
        Y_UNUSED(handshake);

        for (auto i = 0; i < 3; ++i) {
            runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvPoll));

            GrabReadPQRequest(runtime, INITIAL_OFFSET + i * 2);

            runtime.Send(new IEventHandle(reader, pqtablet, GenerateData(i)));

            GrabDataEvent(runtime, i);

            // TODO check commit
        }
    }

    Y_UNIT_TEST(Booting) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        TActorId pqtablet = runtime.AllocateEdgeActor();
        TActorId reader = runtime.Register(CreateLocalPartitionReader(pqtablet, PARTITION));
        runtime.EnableScheduleForActor(reader);
        TActorId worker = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvHandshake));

        for (int i = 0; i < 5; ++i) {
            GrabInitialPQRequest(runtime);

            auto* getOffsetResponse = new TEvPersQueue::TEvResponse;
            getOffsetResponse->Record.SetErrorCode(NPersQueue::NErrorCode::INITIALIZING);
            runtime.Send(new IEventHandle(reader, pqtablet, getOffsetResponse));
        }

        GrabInitialPQRequest(runtime);

        auto* getOffsetResponse = new TEvPersQueue::TEvResponse;
        getOffsetResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        getOffsetResponse->Record.MutablePartitionResponse()->MutableCmdGetClientOffsetResult()->SetOffset(INITIAL_OFFSET);
        runtime.Send(new IEventHandle(reader, pqtablet, getOffsetResponse));

        auto handshake = runtime.GrabEdgeEventRethrow<TEvWorker::TEvHandshake>(handle);
        Y_UNUSED(handshake);
    }

    Y_UNIT_TEST(FeedSlowly) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        TActorId pqtablet = runtime.AllocateEdgeActor();
        TActorId reader = runtime.Register(CreateLocalPartitionReader(pqtablet, PARTITION));
        runtime.EnableScheduleForActor(reader);
        TActorId worker = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvHandshake));

        GrabInitialPQRequest(runtime);

        auto* getOffsetResponse = new TEvPersQueue::TEvResponse;
        getOffsetResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        getOffsetResponse->Record.MutablePartitionResponse()->MutableCmdGetClientOffsetResult()->SetOffset(INITIAL_OFFSET);
        runtime.Send(new IEventHandle(reader, pqtablet, getOffsetResponse));

        auto handshake = runtime.GrabEdgeEventRethrow<TEvWorker::TEvHandshake>(handle);
        Y_UNUSED(handshake);

        runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvPoll));

        for (auto i = 0; i < 3; ++i) {
            GrabReadPQRequest(runtime, INITIAL_OFFSET);

            runtime.SimulateSleep(TDuration::Seconds(1));

            runtime.Send(new IEventHandle(reader, pqtablet, GenerateEmptyData()));
        }

        for (auto i = 0; i < 3; ++i) {
            if (i != 0) {
                runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvPoll));
            }

            GrabReadPQRequest(runtime, INITIAL_OFFSET + i * 2);

            runtime.Send(new IEventHandle(reader, pqtablet, GenerateData(i)));

            GrabDataEvent(runtime, i);

            // TODO check commit
        }

        runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvPoll));

        for (auto i = 0; i < 2; ++i) {
            GrabReadPQRequest(runtime, 9);

            runtime.SimulateSleep(TDuration::Seconds(1));

            runtime.Send(new IEventHandle(reader, pqtablet, GenerateEmptyData()));
        }

        for (auto i = 3; i < 5; ++i) {
            if (i != 3) {
                runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvPoll));
            }

            GrabReadPQRequest(runtime, INITIAL_OFFSET + i * 2);

            runtime.Send(new IEventHandle(reader, pqtablet, GenerateData(i)));

            GrabDataEvent(runtime, i);

            // TODO check commit
        }
    }

    // TODO write tests with real PQ
}

} // namespace NKikimr::NBackup::NImpl
