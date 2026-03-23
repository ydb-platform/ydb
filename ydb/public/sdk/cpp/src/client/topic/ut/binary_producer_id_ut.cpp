#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/managed_executor.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/topic/common/executor_impl.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>
#include <ydb/public/sdk/cpp/src/client/topic/impl/write_session.h>

#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/ut/common/pq_ut_common.h>
#include <ydb/core/persqueue/writer/source_id_encoding.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>
#include <library/cpp/string_utils/base64/base64.h>

#include <atomic>
#include <util/stream/zlib.h>


using namespace std::chrono_literals;

static const bool EnableDirectRead = !std::string{std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") ? std::getenv("PQ_EXPERIMENTAL_DIRECT_READ") : ""}.empty();


namespace NYdb::inline Dev::NTopic::NTests {

// Serialize data in the format expected by the PQ tablet (TDataChunk proto)
TString SerializeDataChunk(ui64 seqNo, const TString& payload) {
    NKikimrPQClient::TDataChunk proto;
    proto.SetSeqNo(seqNo);
    proto.SetData(payload);
    proto.SetCodec(0);  // RAW codec
    proto.SetCreateTime(TInstant::Now().MilliSeconds());
    TString result;
    Y_PROTOBUF_SUPPRESS_NODISCARD proto.SerializeToString(&result);
    return result;
}

// Write a message with binary (non-UTF8) producer ID using direct tablet communication
// This bypasses gRPC string validation by sending directly to the PQ tablet
// The SourceId field in TCmdWrite is defined as 'bytes' in protobuf, so it supports binary data
void WriteBinaryProducerIdWithDirectTabletWrite(TTopicSdkTestSetup& setup,
                                                 const TString& topicPath,
                                                 const TString& binaryProducerId,
                                                 const TString& payload) {
    auto& runtime = setup.GetRuntime();
    NActors::TActorId edge = runtime.AllocateEdgeActor();

    // Get the tablet ID for partition 0 using scheme cache navigation
    auto navigate = std::make_unique<NKikimr::NSchemeCache::TSchemeCacheNavigate>();
    navigate->DatabaseName = "/Root";

    NKikimr::NSchemeCache::TSchemeCacheNavigate::TEntry entry;
    entry.Path = NKikimr::SplitPath(topicPath);
    entry.SyncVersion = true;
    entry.ShowPrivatePath = true;
    entry.Operation = NKikimr::NSchemeCache::TSchemeCacheNavigate::OpList;

    navigate->ResultSet.push_back(std::move(entry));
    navigate->Cookie = 12345;

    runtime.Send(NKikimr::MakeSchemeCacheID(), edge,
                 new NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySet(navigate.release()),
                 0,
                 true);
    auto response = runtime.GrabEdgeEvent<NKikimr::TEvTxProxySchemeCache::TEvNavigateKeySetResult>();

    UNIT_ASSERT_VALUES_EQUAL(response->Request->Cookie, 12345);
    UNIT_ASSERT_VALUES_EQUAL(response->Request->ErrorCount, 0);

    auto& front = response->Request->ResultSet.front();
    UNIT_ASSERT(front.PQGroupInfo);
    UNIT_ASSERT_GT(front.PQGroupInfo->Description.PartitionsSize(), 0);

    ui64 tabletId = 0;
    for (size_t i = 0; i < front.PQGroupInfo->Description.PartitionsSize(); ++i) {
        auto& p = front.PQGroupInfo->Description.GetPartitions(i);
        if (p.GetPartitionId() == 0) {
            tabletId = p.GetTabletId();
            break;
        }
    }
    UNIT_ASSERT(tabletId != 0);

    // First, get ownership of the partition
    auto [ownerCookie, pipeClient] = NKikimr::NPQ::CmdSetOwner(&runtime, tabletId, edge, 0, "test-owner", true);

    // Encode the binary producer ID using the same encoding as the mirrorer
    TString encodedSourceId = NKikimr::NPQ::NSourceIdEncoding::EncodeSimple(binaryProducerId);

    // Serialize the data in TDataChunk format (as expected by the PQ tablet)
    TString serializedData = SerializeDataChunk(1, payload);

    // Build the write request manually to have full control over the SourceId field
    THolder<NKikimr::TEvPersQueue::TEvRequest> request;
    request.Reset(new NKikimr::TEvPersQueue::TEvRequest);
    auto req = request->Record.MutablePartitionRequest();
    req->SetPartition(0);
    req->SetOwnerCookie(ownerCookie);
    req->SetMessageNo(0);
    req->SetCmdWriteOffset(0);

    auto write = req->AddCmdWrite();
    write->SetSourceId(encodedSourceId);
    write->SetSeqNo(1);
    write->SetData(serializedData);
    write->SetCreateTimeMS(TInstant::Now().MilliSeconds());
    write->SetDisableDeduplication(true);

    runtime.SendToPipe(tabletId, edge, request.Release(), 0, NKikimr::GetPipeConfigWithRetries());

    // Wait for the response
    TAutoPtr<NActors::IEventHandle> handle;
    auto* result = runtime.GrabEdgeEvent<NKikimr::TEvPersQueue::TEvResponse>(handle);
    UNIT_ASSERT(result);
    UNIT_ASSERT_C(result->Record.GetErrorCode() == ::NPersQueue::NErrorCode::OK,
                  "Write failed: " << result->Record.GetErrorReason());
    UNIT_ASSERT_VALUES_EQUAL(result->Record.GetPartitionResponse().CmdWriteResultSize(), 1);
}

Y_UNIT_TEST_SUITE(ReadBinaryProducerId) {
    Y_UNIT_TEST(ReadBinaryProducerId) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME};

        const std::string topicPath = setup.GetTopicPath();
        const TString fullTopicPath = setup.GetFullTopicPath();
        const TString binaryProducerId("\xFF\xFE\xF0", 3);
        const TString payload = "payload";

        // Use direct tablet communication to write with binary producer ID
        // This bypasses gRPC string validation - SourceId is 'bytes' type in protobuf
        WriteBinaryProducerIdWithDirectTabletWrite(setup, fullTopicPath, binaryProducerId, payload);

        auto client = setup.MakeClient();
        NThreading::TPromise<void> gotPromise = NThreading::NewPromise<void>();
        TString gotProducerId;
        TString gotMetaProducerId;
        std::atomic<bool> handled{false};

        auto readSettings = TReadSessionSettings()
            // .DirectRead(EnableDirectRead)
            .ConsumerName(setup.GetConsumerName())
            .AppendTopics(topicPath);

        readSettings.EventHandlers_.SimpleDataHandlers([&](TReadSessionEvent::TDataReceivedEvent& ev) {
            if (handled.exchange(true)) {
                ev.Commit();
                return;
            }

            UNIT_ASSERT(!ev.GetMessages().empty());
            auto& msg = ev.GetMessages().front();
            gotProducerId = msg.GetProducerId();
            auto meta = msg.GetMeta();
            UNIT_ASSERT(meta);
            auto it = meta->Fields.find("_encoded_producer_id");
            UNIT_ASSERT(it != meta->Fields.end());
            gotMetaProducerId = it->second;
            UNIT_ASSERT_VALUES_EQUAL(TString{msg.GetData()}, payload);
            msg.Commit();
            gotPromise.SetValue();
        });

        auto reader = client.CreateReadSession(readSettings);
        UNIT_ASSERT(gotPromise.GetFuture().Wait(TDuration::Seconds(10)));
        reader->Close(TDuration::Seconds(5));

        const TString expectedEncoded = Base64Encode(binaryProducerId);
        UNIT_ASSERT_VALUES_EQUAL(gotProducerId, expectedEncoded);
        UNIT_ASSERT_VALUES_EQUAL(gotMetaProducerId, expectedEncoded);
        UNIT_ASSERT_VALUES_EQUAL(Base64Decode(expectedEncoded), binaryProducerId);
    }
} // Y_UNIT_TEST_SUITE(BasicUsage)

} // namespace
