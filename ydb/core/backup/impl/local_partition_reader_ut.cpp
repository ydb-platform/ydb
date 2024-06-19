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
    Y_UNIT_TEST(Simple) {
        TTestActorRuntime runtime;
        runtime.Initialize(NKikimr::TAppPrepare().Unwrap());

        TActorId pqtablet = runtime.AllocateEdgeActor();
        TActorId reader = runtime.Register(CreateLocalPartitionReader(pqtablet, 7));
        TActorId worker = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvHandshake));

        auto getOffset = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvRequest>(handle);
        UNIT_ASSERT(getOffset->Record.HasPartitionRequest());
        UNIT_ASSERT_VALUES_EQUAL(getOffset->Record.GetPartitionRequest().GetPartition(), 7);
        UNIT_ASSERT(getOffset->Record.GetPartitionRequest().HasCmdGetClientOffset());
        auto getOffsetCmd = getOffset->Record.GetPartitionRequest().GetCmdGetClientOffset();
        UNIT_ASSERT_VALUES_EQUAL(getOffsetCmd.GetClientId(), "__OFFLOAD_ACTOR__"); // hardcoded for now

        auto* getOffsetResponse = new TEvPersQueue::TEvResponse;
        getOffsetResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
        getOffsetResponse->Record.MutablePartitionResponse()->MutableCmdGetClientOffsetResult()->SetOffset(3);
        runtime.Send(new IEventHandle(reader, pqtablet, getOffsetResponse));

        auto handshake = runtime.GrabEdgeEventRethrow<TEvWorker::TEvHandshake>(handle);
        Y_UNUSED(handshake);

        for (auto i = 0; i < 3; ++i) {
            runtime.Send(new IEventHandle(reader, worker, new TEvWorker::TEvPoll));

            auto read = runtime.GrabEdgeEventRethrow<TEvPersQueue::TEvRequest>(handle);
            UNIT_ASSERT(read->Record.HasPartitionRequest());
            UNIT_ASSERT_VALUES_EQUAL(read->Record.GetPartitionRequest().GetPartition(), 7);
            UNIT_ASSERT(read->Record.GetPartitionRequest().HasCmdRead());
            auto readCmd = read->Record.GetPartitionRequest().GetCmdRead();
            UNIT_ASSERT_VALUES_EQUAL(readCmd.GetClientId(), "__OFFLOAD_ACTOR__"); // hardcoded for now
            UNIT_ASSERT_VALUES_EQUAL(readCmd.GetOffset(), 3 + i * 2);

            auto* readResponse = new TEvPersQueue::TEvResponse;
            readResponse->Record.SetErrorCode(NPersQueue::NErrorCode::OK);
            auto& cmdReadResult = *readResponse->Record.MutablePartitionResponse()->MutableCmdReadResult();
            auto& readResult1 = *cmdReadResult.AddResult();
            NKikimrPQClient::TDataChunk msg1;
            msg1.SetData(Sprintf("1-%d", i));
            TString msg1Str;
            UNIT_ASSERT(msg1.SerializeToString(&msg1Str));
            readResult1.SetOffset(3 + i * 2);
            readResult1.SetData(msg1Str);
            auto& readResult2 = *cmdReadResult.AddResult();
            NKikimrPQClient::TDataChunk msg2;
            msg2.SetData(Sprintf("2-%d", i));
            TString msg2Str;
            UNIT_ASSERT(msg2.SerializeToString(&msg2Str));
            readResult2.SetOffset(3 + i * 2 + 1);
            readResult2.SetData(msg2Str);
            runtime.Send(new IEventHandle(reader, pqtablet, readResponse));

            auto data = runtime.GrabEdgeEventRethrow<TEvWorker::TEvData>(handle);
            UNIT_ASSERT_VALUES_EQUAL(data->Source, "7");
            UNIT_ASSERT_VALUES_EQUAL(data->Records.size(), 2);
            UNIT_ASSERT_VALUES_EQUAL(data->Records[0].Offset, 3 + i * 2);
            UNIT_ASSERT_VALUES_EQUAL(data->Records[0].Data, Sprintf("1-%d", i));
            UNIT_ASSERT_VALUES_EQUAL(data->Records[1].Offset, 3 + i * 2 + 1);
            UNIT_ASSERT_VALUES_EQUAL(data->Records[1].Data, Sprintf("2-%d", i));

            // TODO check commit
        }
    }

    // TODO write tests with real PQ
}

} // namespace NKikimr::NBackup::NImpl
