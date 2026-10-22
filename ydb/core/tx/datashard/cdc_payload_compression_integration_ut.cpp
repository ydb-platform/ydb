#include "defs.h"
#include "datashard_ut_common_kqp.h"
#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include <ydb/core/persqueue/events/global.h>
#include <ydb/core/persqueue/writer/writer.h>
#include <ydb/core/protos/grpc_pq_old.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/schemeshard/operations.pb.h>
#include <ydb/core/testlib/tablet_helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/codecs.h>
#include <library/cpp/json/json_reader.h>
#include <library/cpp/json/json_writer.h>
#include <library/cpp/string_utils/base64/base64.h>
#include <library/cpp/threading/future/async.h>
#include <util/stream/str.h>
#include <util/stream/zlib.h>
#include <util/string/builder.h>

#include <memory>

namespace NKikimr {
namespace {
using namespace Tests;
using namespace NDataShard;
using namespace NDataShard::NKqpHelpers;
using namespace NSchemeShardUT_Private;

TString CompressionNoise(size_t size) {
    TString result(size, '\0');
    ui64 state = 0x6a09e667f3bcc909ULL;
    for (auto& ch : result) {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        ch = static_cast<char>(state & 255);
    }
    return result;
}

TString CompressionDecode(const NKikimrPQClient::TDataChunk& chunk) {
    if (chunk.GetCodec() == 0) {
        return chunk.GetData();
    }
    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 1);
    NYdb::NTopic::TGzipCodec gzip;
    const NYdb::NTopic::ICodec& codec = gzip;
    return TString(codec.Decompress(std::string(chunk.GetData().data(), chunk.GetData().size())));
}

class TCompressionSourceEnv {
public:
    TPortManager Ports;
    TServer::TPtr Server;
    TActorId Edge;
    ui64 Shard = 0;
    THolder<NYdb::TDriver> Driver;
    THolder<NYdb::NTopic::TTopicClient> Client;
    TAdaptiveThreadPool Pool;
    TVector<std::shared_ptr<NYdb::NTopic::IReadSession>> Readers;

    TTestActorRuntime& Runtime() { return *Server->GetRuntime(); }

    template <typename TFunc>
    auto Call(TFunc&& func, TDuration simTimeout = TDuration::Seconds(60)) {
        return Runtime().WaitFuture(NThreading::Async(std::forward<TFunc>(func), Pool), simTimeout);
    }

    TCompressionSourceEnv() {
        NKikimrPQ::TPQConfig pq;
        pq.SetEnabled(true);
        pq.SetEnableProtoSourceIdInfo(true);
        pq.SetTopicsAreFirstClassCitizen(true);
        pq.SetCheckACL(false);
        pq.SetRequireCredentialsInNewProtocol(false);
        pq.MutableQuotingConfig()->SetEnableQuoting(false);
        auto settings = TServerSettings(Ports.GetPort(), {}, pq).SetUseRealThreads(false)
            .SetDomainName("Root").SetEnableChangefeedInitialScan(true);
        settings.AppConfig->MutableDataShardConfig()->SetCdcPayloadCompression(
            NKikimrConfig::TDataShardConfig::CDC_PAYLOAD_COMPRESSION_GZIP);
        Server = new TServer(settings);
        const auto grpc = Ports.GetPort();
        Server->EnableGRpc(grpc);
        Edge = Runtime().AllocateEdgeActor();
        InitRoot(Server, Edge);
        const auto created = CreateShardedTable(Server, Edge, "/Root", "Table", TShardedTableOptions().Columns({
            {"key", "Uint32", true, false}, {"value", "Utf8", false, false},
            {"binary", "String", false, false}}));
        Shard = std::get<0>(created).front();
        Pool.Start();
        // Driver construction performs synchronous discovery. Dispatch actor
        // traffic while it waits, just as for subsequent SDK operations.
        Call([&] {
            Driver = MakeHolder<NYdb::TDriver>(NYdb::TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << grpc).SetDatabase("/Root"));
            Client = MakeHolder<NYdb::NTopic::TTopicClient>(*Driver);
        });
    }

    ~TCompressionSourceEnv() noexcept {
        // Assertion unwinding must retain the primary test failure. SDK close
        // and server shutdown have real deadlines; pump their actor traffic.
        try {
            Shutdown();
        } catch (const std::exception& error) {
            Cerr << "Compression fixture cleanup failed: " << error.what() << Endl;
        } catch (...) {
            Cerr << "Compression fixture cleanup failed with an unknown exception" << Endl;
        }
    }

    auto CreateReader(const NYdb::NTopic::TReadSessionSettings& settings) {
        auto reader = Client->CreateReadSession(settings);
        Readers.push_back(reader);
        return reader;
    }

    bool CloseReader(const std::shared_ptr<NYdb::NTopic::IReadSession>& reader) {
        const bool closed = Call([&] { return reader->Close(TDuration::Seconds(5)); });
        for (auto it = Readers.begin(); it != Readers.end(); ++it) {
            if (*it == reader) {
                Readers.erase(it);
                break;
            }
        }
        return closed;
    }

    void Shutdown() {
        for (const auto& reader : Readers) {
            if (!Call([&] { return reader->Close(TDuration::Seconds(5)); }, TDuration::Max())) {
                Cerr << "Compression fixture reader cleanup did not close gracefully" << Endl;
            }
        }
        Readers.clear();
        Client.Reset();
        if (Driver) {
            Call([&] { Driver->Stop(false); }, TDuration::Max());
        }
        // Stop() has a real shutdown deadline. Keep the simulated runtime
        // dispatching cancellation callbacks while it waits for active RPCs.
        Call([&] { Server->ShutdownGRpc(); }, TDuration::Max());
        Driver.Reset();
    }

    void CreateStream(const TShardedTableOptions::TCdcStream& stream) {
        auto attributes = Runtime().AddObserver<TEvTxUserProxy::TEvProposeTransaction>([&](auto& ev) {
            auto* tx = ev->Get()->Record.MutableTransaction()->MutableModifyScheme();
            if (tx->GetOperationType() == NKikimrSchemeOp::ESchemeOpCreateCdcStream) {
                auto* attr = tx->MutableCreateCdcStream()->MutableStreamDescription()->AddUserAttributes();
                attr->SetKey("__async_replication");
                attr->SetValue(R"({"supported_payload_codecs":["gzip"]})");
            }
        });
        WaitTxNotification(Server, Edge, AsyncAlterAddStream(Server, "/Root", "Table", stream));
    }

    ui64 ResolveCompressionTopicTablet() {
        const TString path = "/Root/Table/Stream";
        const auto stream = Ls(Runtime(), Edge, path);
        UNIT_ASSERT(stream->ResultSet.at(0).ListNodeEntry);
        const auto& children = stream->ResultSet.at(0).ListNodeEntry->Children;
        UNIT_ASSERT_VALUES_EQUAL(children.size(), 1);
        const auto topic = Navigate(Runtime(), Edge, TStringBuilder() << path << '/' << children.front().Name,
            NSchemeCache::TSchemeCacheNavigate::EOp::OpTopic);
        UNIT_ASSERT(topic->ResultSet.at(0).PQGroupInfo);
        for (const auto& partition : topic->ResultSet.at(0).PQGroupInfo->Description.GetPartitions()) {
            if (partition.GetPartitionId() == 0) {
                return partition.GetTabletId();
            }
        }
        UNIT_FAIL("Cannot resolve compression topic partition0");
        return 0;
    }

    NKikimrClient::TCmdReadResult ReadStored() {
        auto request = MakeHolder<TEvPersQueue::TEvRequest>();
        auto* partition = request->Record.MutablePartitionRequest();
        partition->SetTopic("/Root/Table/Stream");
        partition->SetPartition(0);
        auto* read = partition->MutableCmdRead();
        read->SetClientId(NPQ::CLIENTID_WITHOUT_CONSUMER);
        read->SetCount(100);
        read->SetBytes(8 * 1024 * 1024);
        read->SetOffset(0);
        read->SetReadToBlobEnd(true);
        read->SetReadTimestampMs(0);
        read->SetExternalOperation(true);
        ForwardToTablet(Runtime(), ResolveCompressionTopicTablet(), Edge, request.Release());
        const auto response = Runtime().GrabEdgeEventRethrow<TEvPersQueue::TEvResponse>(Edge);
        UNIT_ASSERT_VALUES_EQUAL(static_cast<i32>(response->Get()->Record.GetErrorCode()), static_cast<i32>(NPersQueue::NErrorCode::OK));
        return response->Get()->Record.GetPartitionResponse().GetCmdReadResult();
    }
};

} // anonymous namespace

Y_UNIT_TEST_SUITE(CdcPayloadCompressionIntegration) {
    Y_UNIT_TEST(PayloadCompressionRetryProducesSameLogicalRecord) {
        TCompressionSourceEnv env;
        auto& runtime = env.Runtime();
        env.CreateStream({.Name = "Stream", .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatJson});
        TVector<NKikimrClient::TPersQueuePartitionRequest::TCmdWrite> requests;
        THolder<IEventHandle> heldRequest;
        TActorId firstSender;
        TActorId firstWriter;
        auto requestObserver = runtime.AddObserver<NPQ::TEvPartitionWriter::TEvWriteRequest>([&](auto& ev) {
            bool hasData = false;
            for (const auto& cmd : ev->Get()->Record.GetPartitionRequest().GetCmdWrite()) {
                if (cmd.HasData()) {
                    requests.push_back(cmd);
                    hasData = true;
                }
            }
            if (hasData && !firstWriter) {
                firstSender = ev->Sender;
                firstWriter = ev->GetRecipientRewrite();
                heldRequest.Reset(ev.Release());
            }
        });
        THolder<IEventHandle> heldResponse;
        auto responseObserver = runtime.AddObserver<NPQ::TEvPartitionWriter::TEvWriteResponse>([&](auto& ev) {
            if (!heldResponse && ev->Get()->IsSuccess()) {
                heldResponse.Reset(ev.Release());
            }
        });
        const TString large(16384, 'Q');
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "UPSERT INTO `/Root/Table` (key,value) VALUES (1,'" << large << "');");
        runtime.WaitFor("real write held before persistence", [&] { return bool(heldRequest); }, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(requests.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(env.ReadStored().ResultSize(), 0);
        UNIT_ASSERT(!heldResponse);
        const auto original = requests.front();
        NKikimrPQClient::TDataChunk originalChunk;
        UNIT_ASSERT(originalChunk.ParseFromString(original.GetData()));
        UNIT_ASSERT_VALUES_EQUAL(originalChunk.GetCodec(), 1);

        // A pre-write failure must cause a new real attempt from the recovered
        // durable source queue. Retire both old actors before dropping the request.
        RebootTablet(runtime, env.Shard, env.Edge);
        runtime.WaitFor("old source sender and writer retired before retry", [&] {
            return !runtime.FindActor(firstSender) && !runtime.FindActor(firstWriter);
        }, TDuration::Seconds(30));
        heldRequest.Reset();
        runtime.WaitFor("actual retry persisted with its real success acknowledgement held", [&] {
            size_t attempts = 0;
            for (const auto& request : requests) {
                attempts += request.GetSeqNo() == original.GetSeqNo();
            }
            return attempts >= 2 && bool(heldResponse);
        }, TDuration::Seconds(60));
        const auto storedBeforeRestart = env.ReadStored();
        UNIT_ASSERT_VALUES_EQUAL(storedBeforeRestart.ResultSize(), 1);
        const auto durableSender = heldResponse->GetRecipientRewrite();
        const auto durableWriter = heldResponse->Sender;
        TActorId recoveredSender;
        TActorId recoveredWriter;
        auto initObserver = runtime.AddObserver<NPQ::TEvPartitionWriter::TEvInitResult>([&](auto& ev) {
            if (ev->Get()->IsSuccess() && ev->Sender != durableWriter
                && ev->GetRecipientRewrite() != durableSender
                && ev->Get()->GetResult().SourceIdInfo.GetSeqNo() >= original.GetSeqNo())
            {
                UNIT_ASSERT(ev->Get()->GetResult().SourceIdInfo.GetExplicit());
                recoveredSender = ev->GetRecipientRewrite();
                recoveredWriter = ev->Sender;
            }
        });
        // Once the topic write is durable, recovery may use the genuine init
        // MaxSeqNo instead of retransmitting an already written logical record.
        RebootTablet(runtime, env.Shard, env.Edge);
        runtime.WaitFor("new generation recovered actual durable MaxSeqNo", [&] {
            return !runtime.FindActor(durableSender) && !runtime.FindActor(durableWriter)
                && bool(recoveredSender) && bool(recoveredWriter);
        }, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_UNEQUAL(recoveredSender, durableSender);
        UNIT_ASSERT_VALUES_UNEQUAL(recoveredWriter, durableWriter);
        const auto storedAfterRecovery = env.ReadStored();
        UNIT_ASSERT_VALUES_EQUAL(storedAfterRecovery.ResultSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(storedAfterRecovery.GetResult(0).GetData(), storedBeforeRestart.GetResult(0).GetData());
        responseObserver.Remove();
        // Release the real obsolete acknowledgement only after its recipient
        // retired; no new-generation completion is synthesized.
        runtime.Send(heldResponse.Release());
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "UPSERT INTO `/Root/Table` (key,value) VALUES (2,'" << large << "');");
        NKikimrClient::TCmdReadResult history;
        runtime.WaitFor("recovered source queue progressed", [&] {
            history = env.ReadStored();
            return history.ResultSize() >= 2;
        }, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(history.ResultSize(), 2);
        for (size_t i = 0; i < 2; ++i) {
            const auto& item = history.GetResult(i);
            UNIT_ASSERT_VALUES_EQUAL(item.GetOffset(), i);
            UNIT_ASSERT_VALUES_EQUAL(item.GetSourceId(), original.GetSourceId());
            NKikimrPQClient::TDataChunk chunk;
            UNIT_ASSERT(chunk.ParseFromString(item.GetData()));
            UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 1);
            const auto body = CompressionDecode(chunk);
            UNIT_ASSERT_VALUES_EQUAL(item.GetUncompressedSize(), body.size());
            NJson::TJsonValue json;
            UNIT_ASSERT(NJson::ReadJsonTree(body, &json));
            UNIT_ASSERT_VALUES_EQUAL(json["key"][0].GetUInteger(), i + 1);
            UNIT_ASSERT_VALUES_EQUAL(json["update"]["value"].GetString(), large);
        }
        UNIT_ASSERT_VALUES_EQUAL(history.GetResult(0).GetSeqNo(), original.GetSeqNo());
        UNIT_ASSERT_VALUES_EQUAL(history.GetResult(0).GetPartitionKey(), original.GetPartitionKey());
        UNIT_ASSERT_VALUES_EQUAL(history.GetResult(0).GetData(), storedBeforeRestart.GetResult(0).GetData());
        for (const auto& request : requests) {
            if (request.GetSeqNo() == original.GetSeqNo()) {
                UNIT_ASSERT_VALUES_EQUAL(request.SerializeAsString(), original.SerializeAsString());
            }
        }
    }

    Y_UNIT_TEST(PayloadCompressionSchemaBarrierAndHeartbeat) {
        TCompressionSourceEnv env;
        auto& runtime = env.Runtime();
        TVector<NKikimrClient::TPersQueuePartitionRequest::TCmdWrite> writes;
        auto observer = runtime.AddObserver<NPQ::TEvPartitionWriter::TEvWriteRequest>([&](auto& ev) {
            for (const auto& cmd : ev->Get()->Record.GetPartitionRequest().GetCmdWrite()) {
                writes.push_back(cmd);
            }
        });
        env.CreateStream({.Name = "Stream", .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatJson, .VirtualTimestamps = true,
            .ResolvedTimestamps = TDuration::Seconds(3), .SchemaChanges = true});
        const TString large(16384, 'Q');
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "UPSERT INTO `/Root/Table` (key,value) VALUES (1,'" << large << "');");
        ExecSQL(env.Server, env.Edge, "UPSERT INTO `/Root/Table` (key,value) VALUES (2,'small');");
        WaitTxNotification(env.Server, env.Edge, AsyncAlterAddExtraColumn(env.Server, "/Root", "Table"));
        RebootTablet(runtime, env.Shard, env.Edge);
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "UPSERT INTO `/Root/Table` (key,value,extra) VALUES (3,'" << large << "',17);");
        TRowVersion lastDataVersion;
        bool seenSchema = false;
        size_t dataCount = 0;
        runtime.WaitFor("data, schema and following resolved heartbeat", [&] {
            dataCount = 0;
            seenSchema = false;
            lastDataVersion = TRowVersion::Min();
            bool resolvedAfterData = false;
            for (const auto& cmd : writes) {
                NKikimrPQClient::TDataChunk chunk;
                if (cmd.HasData()) {
                    UNIT_ASSERT(chunk.ParseFromString(cmd.GetData()));
                    NJson::TJsonValue json;
                    UNIT_ASSERT(NJson::ReadJsonTree(CompressionDecode(chunk), &json));
                    const auto key = json["key"][0].GetUInteger();
                    UNIT_ASSERT_VALUES_EQUAL(key, dataCount + 1);
                    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), key == 2 ? 0 : 1);
                    UNIT_ASSERT_VALUES_EQUAL(json["ts"].GetArraySafe().size(), 2);
                    lastDataVersion = TRowVersion(json["ts"][0].GetUInteger(), json["ts"][1].GetUInteger());
                    UNIT_ASSERT(lastDataVersion > TRowVersion::Min());
                    UNIT_ASSERT_VALUES_EQUAL(json["update"]["value"].GetString(), key == 2 ? TString("small") : large);
                    if (key == 3) {
                        UNIT_ASSERT(seenSchema);
                        UNIT_ASSERT_VALUES_EQUAL(json["update"]["extra"].GetUInteger(), 17);
                    }
                    ++dataCount;
                } else if (cmd.HasSchemaChange()) {
                    UNIT_ASSERT(chunk.ParseFromString(cmd.GetSchemaChange().GetData()));
                    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
                    UNIT_ASSERT(!cmd.HasUncompressedSize());
                    NJson::TJsonValue json;
                    UNIT_ASSERT(NJson::ReadJsonTree(chunk.GetData(), &json));
                    UNIT_ASSERT_VALUES_EQUAL(json["ts"].GetArraySafe().size(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(json["ts"][0].GetUInteger(), cmd.GetSchemaChange().GetStep());
                    UNIT_ASSERT_VALUES_EQUAL(json["ts"][1].GetUInteger(), cmd.GetSchemaChange().GetTxId());
                    UNIT_ASSERT(dataCount >= 2);
                    UNIT_ASSERT_VALUES_EQUAL(json["tableChanges"][0]["table"]["columns"]["extra"]["type"].GetString(), "Uint32");
                    seenSchema = true;
                } else if (cmd.HasHeartbeat()) {
                    UNIT_ASSERT(chunk.ParseFromString(cmd.GetHeartbeat().GetData()));
                    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
                    UNIT_ASSERT(!cmd.HasUncompressedSize());
                    NJson::TJsonValue json;
                    UNIT_ASSERT(NJson::ReadJsonTree(chunk.GetData(), &json));
                    UNIT_ASSERT_VALUES_EQUAL(json["resolved"].GetArraySafe().size(), 2);
                    UNIT_ASSERT_VALUES_EQUAL(json["resolved"][0].GetUInteger(), cmd.GetHeartbeat().GetStep());
                    UNIT_ASSERT_VALUES_EQUAL(json["resolved"][1].GetUInteger(), cmd.GetHeartbeat().GetTxId());
                    const TRowVersion resolved(cmd.GetHeartbeat().GetStep(), cmd.GetHeartbeat().GetTxId());
                    resolvedAfterData |= dataCount == 3 && resolved >= lastDataVersion;
                }
            }
            return dataCount == 3 && seenSchema && resolvedAfterData;
        }, TDuration::Seconds(60));
        // All records must also survive the ordinary persisted topic boundary.
        size_t rows = 0;
        bool storedSchema = false;
        bool storedResolved = false;
        runtime.WaitFor("durable data, schema and following resolved heartbeat", [&] {
            rows = 0;
            storedSchema = false;
            storedResolved = false;
            const auto stored = env.ReadStored();
            for (const auto& item : stored.GetResult()) {
                NKikimrPQClient::TDataChunk chunk;
                UNIT_ASSERT(chunk.ParseFromString(item.GetData()));
                NJson::TJsonValue json;
                UNIT_ASSERT(NJson::ReadJsonTree(CompressionDecode(chunk), &json));
                if (json.Has("key")) {
                    ++rows;
                    UNIT_ASSERT(rows <= 3);
                    UNIT_ASSERT_VALUES_EQUAL(json["key"][0].GetUInteger(), rows);
                    if (rows == 3) {
                        UNIT_ASSERT(storedSchema);
                    }
                } else if (json.Has("tableChanges")) {
                    UNIT_ASSERT_VALUES_EQUAL(rows, 2);
                    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
                    storedSchema = true;
                } else if (json.Has("resolved")) {
                    UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), 0);
                    UNIT_ASSERT_VALUES_EQUAL(json["resolved"].GetArraySafe().size(), 2);
                    const TRowVersion resolved(json["resolved"][0].GetUInteger(), json["resolved"][1].GetUInteger());
                    storedResolved |= rows == 3 && resolved >= lastDataVersion;
                }
            }
            return rows == 3 && storedSchema && storedResolved;
        }, TDuration::Seconds(60));
        UNIT_ASSERT_VALUES_EQUAL(rows, 3);
        UNIT_ASSERT(storedSchema);
        UNIT_ASSERT(storedResolved);
    }

    Y_UNIT_TEST(PayloadCompressionLargeSourceRoundTrip) {
        TCompressionSourceEnv env;
        auto& runtime = env.Runtime();
        const TString repetitive(600 * 1024, 'Q');
        auto noisy = CompressionNoise(600 * 1024);
        for (auto& ch : noisy) {
            ch = 'a' + static_cast<ui8>(ch) % 26;
        }
        const auto binary = CompressionNoise(768 * 1024);
        const auto base64 = Base64Encode(binary);
        const TString padding(128 * 1024, 'P');
        const TVector<TString> expected{
            TStringBuilder() << R"({"update":{"binary":null,"value":")" << repetitive << R"("},"key":[1]})",
            TStringBuilder() << R"({"update":{"value":")" << noisy << R"("},"key":[2]})",
            TStringBuilder() << R"({"update":{"binary":")" << base64 << R"(","value":")" << padding << R"("},"key":[3]})",
        };
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "UPSERT INTO `/Root/Table` (key,value) VALUES (1,'" << repetitive << "');");
        env.CreateStream({.Name = "Stream", .Mode = NKikimrSchemeOp::ECdcStreamModeUpdate,
            .Format = NKikimrSchemeOp::ECdcStreamFormatJson, .InitialState = NKikimrSchemeOp::ECdcStreamStateScan});
        runtime.WaitFor("stored compressed initial scan", [&] { return env.ReadStored().ResultSize() != 0; }, TDuration::Seconds(60));
        runtime.GetAppData().DataShardConfig.ClearCdcPayloadCompression();
        RebootTablet(runtime, env.Shard, env.Edge);
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "UPSERT INTO `/Root/Table` (key,value) VALUES (2,'" << noisy << "');");
        runtime.WaitFor("stored raw multipart update", [&] {
            const auto read = env.ReadStored();
            return read.ResultSize() && read.GetResult(read.ResultSize() - 1).GetOffset() == 1;
        }, TDuration::Seconds(60));
        runtime.GetAppData().DataShardConfig.SetCdcPayloadCompression(
            NKikimrConfig::TDataShardConfig::CDC_PAYLOAD_COMPRESSION_GZIP);
        RebootTablet(runtime, env.Shard, env.Edge);
        ExecSQL(env.Server, env.Edge, TStringBuilder() << "$binary = String::Base64Decode('" << base64
            << "'); UPSERT INTO `/Root/Table` (key,value,binary) VALUES (3,'" << padding << "',$binary);");
        NKikimrClient::TCmdReadResult stored;
        runtime.WaitFor("complete large source history", [&] {
            stored = env.ReadStored();
            return stored.ResultSize() && stored.GetResult(stored.ResultSize() - 1).GetOffset() == 2;
        }, TDuration::Seconds(60));
        // The tablet's standard read proxy reassembles physical parts into
        // one logical TResult and retains the final part number/total count.
        UNIT_ASSERT_VALUES_EQUAL(stored.ResultSize(), 3);
        TVector<TString> envelopes(3);
        TVector<ui32> originalSizes(3);
        for (size_t i = 0; i < 3; ++i) {
            const auto& item = stored.GetResult(i);
            UNIT_ASSERT_VALUES_EQUAL(item.GetOffset(), i);
            if (i == 0) {
                UNIT_ASSERT(item.GetTotalParts() <= 1);
            } else {
                UNIT_ASSERT(item.GetTotalParts() >= 2);
                UNIT_ASSERT_VALUES_EQUAL(item.GetPartNo() + 1, item.GetTotalParts());
                UNIT_ASSERT_VALUES_EQUAL(item.GetTotalSize(), item.GetData().size());
            }
            originalSizes[i] = item.GetUncompressedSize();
            envelopes[i] = item.GetData();
        }
        for (size_t i = 0; i < 3; ++i) {
            NKikimrPQClient::TDataChunk chunk;
            UNIT_ASSERT(chunk.ParseFromString(envelopes[i]));
            UNIT_ASSERT_VALUES_EQUAL(chunk.GetCodec(), i == 1 ? 0 : 1);
            const auto decoded = CompressionDecode(chunk);
            NJson::TJsonValue actualJson, expectedJson;
            UNIT_ASSERT(NJson::ReadJsonTree(decoded, &actualJson));
            UNIT_ASSERT(NJson::ReadJsonTree(expected[i], &expectedJson));
            UNIT_ASSERT(actualJson == expectedJson);
            if (i != 1) {
                UNIT_ASSERT_VALUES_EQUAL(originalSizes[i], decoded.size());
            }
            if (i == 2) {
                UNIT_ASSERT(decoded.size() % stored.GetResult(i).GetTotalParts() != 0);
                UNIT_ASSERT(envelopes[i].size() > 500 * 1024);
                UNIT_ASSERT(decoded.size() <= Max<i32>());
                UNIT_ASSERT_VALUES_EQUAL(actualJson["update"]["binary"].GetString(), base64);
                UNIT_ASSERT_VALUES_EQUAL(actualJson["update"]["value"].GetString(), padding);
            }
        }
        for (const bool decompress : {false, true}) {
            const auto consumer = decompress ? "decoded" : "encoded";
            const auto altered = env.Call([&] { return env.Client->AlterTopic("/Root/Table/Stream",
                NYdb::NTopic::TAlterTopicSettings().BeginAddConsumer(consumer).EndAddConsumer()).GetValueSync(); });
            UNIT_ASSERT_C(altered.IsSuccess(), altered.GetIssues().ToString());
            auto reader = env.CreateReader(NYdb::NTopic::TReadSessionSettings()
                .ConsumerName(consumer).AppendTopics(std::string{"/Root/Table/Stream"}).Decompress(decompress));
            size_t received = 0;
            while (received < 3) {
                auto event = env.Call([&] { return reader->GetEvent(true); });
                UNIT_ASSERT(event);
                if (auto* start = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&*event)) {
                    start->Confirm();
                } else if (auto* data = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&*event)) {
                    const auto check = [&](ui64 offset, const TString& body) {
                        UNIT_ASSERT_VALUES_EQUAL(offset, received);
                        NJson::TJsonValue actualJson, expectedJson;
                        UNIT_ASSERT(NJson::ReadJsonTree(body, &actualJson));
                        UNIT_ASSERT(NJson::ReadJsonTree(expected.at(received), &expectedJson));
                        UNIT_ASSERT(actualJson == expectedJson);
                        ++received;
                    };
                    if (decompress) {
                        for (const auto& message : data->GetMessages()) {
                            check(message.GetOffset(), TString(message.GetData()));
                        }
                    } else {
                        for (const auto& message : data->GetCompressedMessages()) {
                            UNIT_ASSERT(message.GetCodec() == (received == 1 ? NYdb::NTopic::ECodec::RAW : NYdb::NTopic::ECodec::GZIP));
                            const auto offset = message.GetOffset();
                            NKikimrPQClient::TDataChunk chunk;
                            chunk.SetCodec(received == 1 ? 0 : 1);
                            chunk.SetData(message.GetData().data(), message.GetData().size());
                            const auto body = CompressionDecode(chunk);
                            if (received != 1) {
                                UNIT_ASSERT_VALUES_EQUAL(message.GetUncompressedSize(), body.size());
                            }
                            check(offset, body);
                        }
                    }
                } else if (auto* closed = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&*event)) {
                    UNIT_FAIL(closed->DebugString());
                }
            }
            UNIT_ASSERT_VALUES_EQUAL(received, 3);
            UNIT_ASSERT(env.CloseReader(reader));
        }
        env.Shutdown();
    }
}
} // NKikimr
