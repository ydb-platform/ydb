#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/schemeshard_config.pb.h>
#include "ut_helpers.h"
#include <ydb/core/base/table_index.h>
#include <ydb/core/base/scan_admission.h>
#include <ydb/core/tablet_flat/flat_exec_broker.h>

#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <contrib/libs/tcmalloc/tcmalloc/malloc_extension.h>
#include <library/cpp/testing/common/env.h>
#include <ydb/library/testlib/helpers.h>

#include <util/generic/size_literals.h>
#include <util/stream/file.h>
#include <util/stream/format.h>
#include <util/string/cast.h>
#include <util/system/mem_info.h>

namespace NKikimr {
namespace {

class THeapSamplingScope {
public:
    THeapSamplingScope() {
        if (Enabled) {
            // A small interval wastes entire allocator pages on sampled small
            // allocations. Use a separate, unsampled run for byte/RSS totals.
            tcmalloc::MallocExtension::SetProfileSamplingInterval(Profile ? 2_MB : 0);
        }
    }

    ~THeapSamplingScope() {
        if (Enabled) {
            tcmalloc::MallocExtension::SetProfileSamplingInterval(OldInterval);
        }
    }

private:
    const bool Profile = GetTestParam("vector-upload-profile") == "true";
    const bool Enabled = Profile || GetTestParam("vector-upload-measure") == "true";
    const i64 OldInterval = tcmalloc::MallocExtension::GetProfileSamplingInterval();
};

// Opt in with --test-param vector-upload-profile=true. Each line contains
// estimated live bytes, estimated object count, and allocation stack PCs.
// Keep the raw PCs so llvm-symbolizer can resolve inlined frames offline.
void DumpUploadHeap(const TString& stage) {
    const bool profileEnabled = GetTestParam("vector-upload-profile") == "true";
    if (!profileEnabled && GetTestParam("vector-upload-measure") != "true") {
        return;
    }
    const auto allocated = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes");
    const auto physical = tcmalloc::MallocExtension::GetNumericProperty("generic.physical_memory_used");
    const auto rss = NMemInfo::GetMemInfo().RSS;
    Cerr << "VECTOR_INDEX_MEMORY stage=" << stage
         << " allocated=" << allocated.value_or(0)
         << " physical=" << physical.value_or(0)
         << " rss=" << rss << Endl;
    {
        TFileOutput stats(GetOutputPath() / (stage + ".stats.tsv"));
        stats << "allocated\t" << allocated.value_or(0) << '\n'
              << "physical\t" << physical.value_or(0) << '\n'
              << "rss\t" << rss << '\n';
    }
    if (!profileEnabled) {
        return;
    }
#if defined(__linux__)
    {
        TFileInput maps("/proc/self/maps");
        TFileOutput out(GetOutputPath() / (stage + ".maps"));
        out << maps.ReadAll();
    }
#endif
    TFileOutput out(GetOutputPath() / (stage + ".heap.tsv"));
    auto profile = tcmalloc::MallocExtension::SnapshotCurrent(tcmalloc::ProfileType::kHeap);
    profile.Iterate([&](const tcmalloc::Profile::Sample& sample) {
        out << sample.sum << '\t' << sample.count;
        for (int i = 0; i < sample.depth; ++i) {
            out << '\t' << Hex(reinterpret_cast<uintptr_t>(sample.stack[i]), HF_ADDX);
        }
        out << '\n';
    });
}

class TAdmissionFixture {
public:
    TPortManager Ports;
    Tests::TServer::TPtr Server;
    TTestActorRuntime& Runtime;
    const TActorId Sender;
    TVector<ui64> Shards;
    TTableId Table;

    TAdmissionFixture()
        : Server(new Tests::TServer(Tests::TServerSettings(Ports.GetPort(2134))
            .SetDomainName("Root").SetUseRealThreads(false)))
        , Runtime(*Server->GetRuntime())
        , Sender(Runtime.AllocateEdgeActor())
    {
        InitRoot(Server, Sender);
        CreateMainTable(Server, Sender, TShardedTableOptions().Shards(2));
        CreatePostingTable(Server, Sender, TShardedTableOptions().Shards(1));
        Shards = GetTableShards(Server, Sender, "/Root/table-main");
        Table = ResolveTableId(Server, Sender, "/Root/table-main");
    }

    auto Request(ui64 shard) {
        auto event = MakeHolder<TEvDataShard::TEvReshuffleKMeansRequest>();
        auto& rec = event->Record;
        rec.SetAllowMemoryAdmission(true);
        rec.SetId(1);
        rec.SetSeqNoGeneration(1);
        rec.SetSeqNoRound(1);
        rec.SetTabletId(shard);
        Table.PathId.ToProto(rec.MutablePathId());
        rec.MutableSettings()->set_vector_dimension(2);
        rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        rec.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING);
        rec.SetParent(0);
        rec.SetChild(1);
        rec.AddClusters("ab\2");
        rec.SetEmbeddingColumn("embedding");
        rec.AddDataColumns("data");
        rec.SetDatabaseName("/Root");
        rec.SetOutputName("/Root/table-posting");
        return event;
    }

    auto LocalRequest(ui64 shard) {
        auto request = MakeHolder<TEvDataShard::TEvLocalKMeansRequest>();
        auto& rec = request->Record;
        rec.SetAllowMemoryAdmission(true);
        rec.SetId(1);
        rec.SetSeqNoGeneration(1);
        rec.SetSeqNoRound(1);
        rec.SetTabletId(shard);
        Table.PathId.ToProto(rec.MutablePathId());
        rec.MutableSettings()->set_vector_dimension(2);
        rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        rec.SetK(2);
        rec.SetSeed(1337);
        rec.SetNeedsRounds(3);
        rec.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING);
        rec.SetParentFrom(0);
        rec.SetParentTo(0);
        rec.SetChild(1);
        rec.SetEmbeddingColumn("embedding");
        rec.AddDataColumns("data");
        rec.SetDatabaseName("/Root");
        rec.SetLevelName("/Root/table-level");
        rec.SetOutputName("/Root/table-posting");
        return request;
    }

    void Send(ui64 shard) {
        Runtime.SendToPipe(shard, Sender, Request(shard).Release(), 0, GetPipeConfigWithRetries());
    }

    auto Reply(NKikimrIndexBuilder::EBuildStatus expected) {
        auto event = Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReshuffleKMeansResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(event->Get()->Record.GetStatus(), expected, event->Get()->Record.ShortDebugString());
        return event;
    }

    void InsertRow(const TString& payload = TString(16_KB, 'x')) {
        auto types = std::make_shared<NTxProxy::TUploadTypes>();
        for (const auto& [name, id] : TVector<std::pair<TString, Ydb::Type::PrimitiveTypeId>>{
                {"key", Ydb::Type::UINT32}, {"embedding", Ydb::Type::STRING}, {"data", Ydb::Type::STRING}}) {
            Ydb::Type type;
            type.set_type_id(id);
            types->emplace_back(name, type);
        }
        auto rows = std::make_shared<NTxProxy::TUploadRows>();
        const TString embedding("ab\2");
        const TVector<TCell> key{TCell::Make(ui32(1))};
        const TVector<TCell> values{TCell(embedding.data(), embedding.size()), TCell(payload.data(), payload.size())};
        rows->emplace_back(TSerializedCellVec(key), TSerializedCellVec::Serialize(values));
        Runtime.Register(NTxProxy::CreateUploadRowsInternal(Sender, "/Root", "/Root/table-main", types, std::move(rows)));
        auto reply = Runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, Ydb::StatusIds::SUCCESS, reply->Get()->Issues.ToString());
    }

    auto Usage() { return Runtime.GetAppData().BuildIndexScanAdmission->GetUsage(); }

    void SetMemoryBudget(ui64 total, ui64 active) {
        Runtime.GetAppData().BuildIndexScanAdmission->SetTotalLimit(total, active);
        using TBroker = NResourceBroker::TEvResourceBroker;
        auto event = MakeHolder<TBroker::TEvConfigure>();
        event->Merge = true;
        auto* queue = event->Record.AddQueues();
        queue->SetName("queue_build_index");
        queue->MutableLimit()->SetMemory(active);
        Runtime.Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), Sender, event.Release()));
        const auto reply = Runtime.GrabEdgeEventRethrow<TBroker::TEvConfigureResult>(Sender);
        UNIT_ASSERT(reply->Get()->Record.GetSuccess());
    }
};

} // namespace

Y_UNIT_TEST_SUITE(TVectorIndexUploadMemory) {
    Y_UNIT_TEST(PendingCountLimitAcrossShards) {
        TAdmissionFixture fixture;
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexPendingScansLimit(1);
        using TSubmit = NResourceBroker::TEvResourceBroker::TEvSubmitTask;
        TBlockEvents<TSubmit> blocked(fixture.Runtime, [](const TSubmit::TPtr& event) {
            return event->Get()->Task.Type == "build_index";
        });
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("first scan queued", [&] { return blocked.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        UNIT_ASSERT_GT(fixture.Usage().Bytes, 0);
        UNIT_ASSERT_GT((*blocked.begin())->Get()->Task.RequiredResources[NKikimrResourceBroker::MEMORY], 0);
        fixture.Send(fixture.Shards[1]);
        auto reply = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRetryAfterMs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(blocked.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        blocked.Stop().Unblock();
        fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
    }

    Y_UNIT_TEST(PendingByteLimitAcrossShards) {
        TAdmissionFixture fixture;
        auto request = fixture.Request(fixture.Shards[0]);
        const ui64 bytesLimit = request->Record.SpaceUsedLong() + request->Record.ByteSizeLong() + 4_KB + 1024;
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexPendingBytesLimit(bytesLimit);
        using TSubmit = NResourceBroker::TEvResourceBroker::TEvSubmitTask;
        TBlockEvents<TSubmit> blocked(fixture.Runtime, [](const TSubmit::TPtr& event) {
            return event->Get()->Task.Type == "build_index";
        });
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        fixture.Runtime.WaitFor("first byte reservation queued", [&] { return blocked.size() == 1; });
        fixture.Send(fixture.Shards[1]);
        auto reply = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRetryAfterMs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        UNIT_ASSERT_LE(fixture.Usage().Bytes, bytesLimit);
        blocked.Stop().Unblock();
        fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
    }

    Y_UNIT_TEST(OversizePendingRequestFailsWithoutRetryHint) {
        TAdmissionFixture fixture;
        for (ui64 limit : {0, 1}) {
            fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexPendingBytesLimit(limit);
            fixture.Send(fixture.Shards[0]);
            auto reply = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
            UNIT_ASSERT(!reply->Get()->Record.HasRetryAfterMs());
            UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
            UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
        }
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexPendingBytesLimit(64_MB);
        fixture.Send(fixture.Shards[0]);
        fixture.Reply(NKikimrIndexBuilder::DONE);
    }

    Y_UNIT_TEST(InvalidRequestAndImpossibleScanReservationArePermanent) {
        TAdmissionFixture fixture;
        auto& config = fixture.Runtime.GetAppData().DataShardConfig;
        config.SetBuildIndexRequestBytesLimit(1);
        fixture.Send(fixture.Shards[0]);
        auto first = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
        UNIT_ASSERT(!first->Get()->Record.HasRetryAfterMs());
        config.SetBuildIndexRequestBytesLimit(32_MB);
        config.SetBuildIndexScanMemoryLimit(1);
        fixture.Send(fixture.Shards[1]);
        auto second = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
        UNIT_ASSERT(!second->Get()->Record.HasRetryAfterMs());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
        config.SetBuildIndexScanMemoryLimit(1_GB);
        fixture.Send(fixture.Shards[1]);
        fixture.Reply(NKikimrIndexBuilder::DONE);
    }

    Y_UNIT_TEST(BrokerCapacityRejectionIsPermanent) {
        TAdmissionFixture fixture;
        using TBroker = NResourceBroker::TEvResourceBroker;
        auto configure = [&](ui64 memory) {
            auto event = MakeHolder<TBroker::TEvConfigure>();
            event->Merge = true;
            auto* queue = event->Record.AddQueues();
            queue->SetName("queue_build_index");
            queue->MutableLimit()->SetMemory(memory);
            fixture.Runtime.Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), fixture.Sender, event.Release()));
            const auto reply = fixture.Runtime.GrabEdgeEventRethrow<TBroker::TEvConfigureResult>(fixture.Sender);
            UNIT_ASSERT(reply->Get()->Record.GetSuccess());
        };
        configure(1);
        fixture.Send(fixture.Shards[0]);
        const auto rejected = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
        UNIT_ASSERT(!rejected->Get()->Record.HasRetryAfterMs());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
        configure(448_MB);
        fixture.Send(fixture.Shards[0]);
        fixture.Reply(NKikimrIndexBuilder::DONE);
    }

    Y_UNIT_TEST(RequiredRowCannotRetryBeyondScanCapacity) {
        TAdmissionFixture fixture;
        auto request = fixture.Request(fixture.Shards[0]);
        request->Record.SetRequiredRowBytes(1_GB);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        const auto reply = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
        UNIT_ASSERT(!reply->Get()->Record.HasRetryAfterMs());
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Record.ShortDebugString(), "Minimum index scan reservation");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(UnrelatedBrokerErrorsDoNotCancelGrantedScan) {
        TAdmissionFixture fixture;
        using TBroker = NResourceBroker::TEvResourceBroker;
        TBlockEvents<TBroker::TEvResourceAllocated> grants(fixture.Runtime, [](const auto& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("grant held for unrelated error", [&] { return grants.size() == 1; });
        const auto& grant = *grants.begin();
        const auto client = grant->Recipient;
        const auto task = grant->Get()->TaskId;
        ui32 finishes = 0;
        auto observer = fixture.Runtime.AddObserver<TBroker::TEvFinishTask>([&](auto& event) {
            if (event->Sender == client && event->Get()->TaskId == task) {
                ++finishes;
            }
        });
        for (auto code : {TBroker::TStatus::ALREADY_EXISTS, TBroker::TStatus::TASK_IN_FLY, TBroker::TStatus::TASK_IN_QUEUE}) {
            auto error = MakeHolder<TBroker::TEvTaskOperationError>();
            error->TaskId = task;
            error->Cookie = grant->Get()->Cookie;
            error->Status.Code = code;
            fixture.Runtime.Send(new IEventHandle(client, fixture.Sender, error.Release()));
        }
        fixture.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        UNIT_ASSERT_VALUES_EQUAL(finishes, 0);
        grants.Stop().Unblock();
        fixture.Reply(NKikimrIndexBuilder::DONE);
        fixture.Runtime.WaitFor("successful scan releases reservation", [&] { return finishes == 1; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(LargestValidCentroidPayloadFitsDefaultRequestLimit) {
        TAdmissionFixture fixture;
        // The default budget on a 2 GiB node must support this request without
        // retries or a later increase of the memory limit.
        fixture.SetMemoryBudget(256_MB, 192_MB);
        auto event = fixture.Request(fixture.Shards[0]);
        auto& rec = event->Record;
        rec.MutableSettings()->set_vector_dimension(2048);
        rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        rec.ClearClusters();
        TString centroid(2048 * sizeof(float) + 1, '\0');
        centroid.back() = 1; // FloatVector format byte.
        for (ui32 i = 0; i < 2048; ++i) {
            rec.AddClusters(centroid);
        }
        UNIT_ASSERT_GT(rec.ByteSizeLong(), 16_MB);
        UNIT_ASSERT_LE(rec.ByteSizeLong(), fixture.Runtime.GetAppData().DataShardConfig.GetBuildIndexRequestBytesLimit());
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, event.Release(), 0, GetPipeConfigWithRetries());
        fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
    }

    Y_UNIT_TEST(RecomputeFitsSmallNodeWithQueryExecutionBudget) {
        TAdmissionFixture fixture;
        // Compute scheduler reserves 20% out of the 30% activities budget.
        const ui64 total = 2_GB / 10;
        const ui64 pending = TScanAdmissionControl::CalculatePendingByteLimit(total, 64_MB);
        fixture.SetMemoryBudget(total, total - pending);
        auto request = MakeHolder<TEvDataShard::TEvRecomputeKMeansRequest>();
        auto& rec = request->Record;
        rec.SetAllowMemoryAdmission(true);
        rec.SetId(1);
        rec.SetSeqNoGeneration(1);
        rec.SetSeqNoRound(1);
        rec.SetTabletId(fixture.Shards[0]);
        fixture.Table.PathId.ToProto(rec.MutablePathId());
        rec.MutableSettings()->set_vector_dimension(2048);
        rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT);
        rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        rec.SetParent(0);
        rec.SetEmbeddingColumn("embedding");
        TString centroid(2048 * sizeof(float) + 1, '\0');
        centroid.back() = 1;
        for (ui32 i = 0; i < 1024; ++i) {
            rec.AddClusters(centroid);
        }
        UNIT_ASSERT_GT(rec.ByteSizeLong(), 8_MB);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto reply = fixture.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvRecomputeKMeansResponse>(fixture.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::DONE,
            reply->Get()->Record.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
    }

    Y_UNIT_TEST(LocalScanShrinksBuffersToFitBrokerBudget) {
        TAdmissionFixture fixture;
        fixture.SetMemoryBudget(256_MB, 96_MB);
        fixture.InsertRow();
        CreateLevelTable(fixture.Server, fixture.Sender, TShardedTableOptions().Shards(1));
        ui64 reservation = 0;
        auto observer = fixture.Runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvSubmitTask>([&](auto& event) {
            if (event->Get()->Task.Type == "build_index") {
                reservation = event->Get()->Task.RequiredResources[NKikimrResourceBroker::MEMORY];
            }
        });
        auto request = fixture.LocalRequest(fixture.Shards[0]);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        auto reply = fixture.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvLocalKMeansResponse>(fixture.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::DONE,
            reply->Get()->Record.ShortDebugString());
        UNIT_ASSERT_GT(reservation, 0);
        UNIT_ASSERT_LE(reservation, 96_MB);
        UNIT_ASSERT_GT(reply->Get()->Record.GetMeteringStats().GetUploadRows(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
    }

    Y_UNIT_TEST_TWIN(LocalEmptyResumeRangeCompletes, Admission) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        CreateLevelTable(fixture.Server, fixture.Sender, TShardedTableOptions().Shards(1));
        auto request = fixture.LocalRequest(fixture.Shards[0]);
        request->Record.SetAllowMemoryAdmission(Admission);
        const TCell key = TCell::Make(ui32(1));
        TSerializedTableRange resume{{&key, 1}, false, {&key, 1}, true};
        resume.Serialize(*request->Record.MutableKeyRange());
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        const auto reply = fixture.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvLocalKMeansResponse>(fixture.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::DONE,
            reply->Get()->Record.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetUploadRows(), 0);
        UNIT_ASSERT(reply->Get()->Record.GetIsEmpty());
    }

    Y_UNIT_TEST(ScanShrinksBuffersToFitExplicitScanLimit) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexScanMemoryLimit(32_MB);
        ui64 reservation = 0;
        auto observer = fixture.Runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvSubmitTask>([&](auto& event) {
            if (event->Get()->Task.Type == "build_index") {
                reservation = event->Get()->Task.RequiredResources[NKikimrResourceBroker::MEMORY];
            }
        });
        fixture.Send(fixture.Shards[0]);
        auto reply = fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_GT(reservation, 0);
        UNIT_ASSERT_LE(reservation, 32_MB);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetMeteringStats().GetUploadRows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
    }

    Y_UNIT_TEST(LargeValidRowRetriesWithLargerReservation) {
        TAdmissionFixture fixture;
        fixture.SetMemoryBudget(256_MB, 96_MB);
        auto observer = fixture.Runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvSubmitTask>([&](auto& event) {
            if (event->Get()->Task.Type == "build_index") {
                UNIT_ASSERT_LE(event->Get()->Task.RequiredResources[NKikimrResourceBroker::MEMORY], 96_MB);
            }
        });
        fixture.InsertRow(TString(10_MB, 'x'));
        fixture.Send(fixture.Shards[0]);
        auto reply = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRequiredRowBytes(), 10_MB);
        const ui64 requiredRowBytes = reply->Get()->Record.GetRequiredRowBytes();
        // The reply owns the old broker reservation until the receiver has
        // processed it. Release it before asking the tight budget for a retry.
        reply.Reset();
        auto event = fixture.Request(fixture.Shards[0]);
        event->Record.SetRequiredRowBytes(requiredRowBytes);
        event->Record.SetSeqNoRound(2);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, event.Release(), 0, GetPipeConfigWithRetries());
        const auto done = fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(done->Get()->Record.GetMeteringStats().GetUploadRows(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(TimeoutAndLateGrantReleasePendingReservation) {
        TAdmissionFixture fixture;
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexQueueTimeoutMs(1000);
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        ui32 uploads = 0;
        auto observer = fixture.Runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>([&](auto&) { ++uploads; });
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("scan grant held", [&] { return grants.size() == 1; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        const auto client = (*grants.begin())->Recipient;
        const auto task = (*grants.begin())->Get()->TaskId;
        ui32 finishes = 0;
        auto finishObserver = fixture.Runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvFinishTask>([&](auto& event) {
            if (event->Sender == client && event->Get()->TaskId == task) {
                ++finishes;
            }
        });
        fixture.Runtime.SimulateSleep(TDuration::Seconds(2));
        auto reply = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRetryAfterMs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Bytes, 0);
        grants.Stop().Unblock();
        fixture.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(uploads, 0);
        UNIT_ASSERT_VALUES_EQUAL(finishes, 1);
        fixture.Send(fixture.Shards[0]);
        fixture.Reply(NKikimrIndexBuilder::DONE);
    }

    Y_UNIT_TEST(SnapshotIsRevalidatedAfterWaitingForGrant) {
        TAdmissionFixture fixture;
        const auto snapshot = CreateVolatileSnapshot(fixture.Server, {"/Root/table-main"});
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        auto request = fixture.Request(fixture.Shards[0]);
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        fixture.Runtime.WaitFor("snapshot scan grant held", [&] { return grants.size() == 1; });
        UNIT_ASSERT(DiscardVolatileSnapshot(fixture.Server, {"/Root/table-main"}, snapshot));
        grants.Stop().Unblock();
        auto reply = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Record.ShortDebugString(), "snapshot expired");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(ReplacementGenerationReleasesOldPendingRequest) {
        TAdmissionFixture fixture;
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexPendingScansLimit(1);
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("old generation grant held", [&] { return grants.size() == 1; });
        auto replacement = fixture.Request(fixture.Shards[0]);
        replacement->Record.SetSeqNoRound(2);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, replacement.Release(), 0, GetPipeConfigWithRetries());
        fixture.Runtime.WaitFor("new generation grant held", [&] { return grants.size() == 2; });
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        grants.Stop().Unblock();
        auto reply = fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(reply->Get()->Record.GetRequestSeqNoRound(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(StaleSequenceGetsReplyWithoutCancellingNewerScan) {
        TAdmissionFixture fixture;
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        auto request = fixture.Request(fixture.Shards[0]);
        request->Record.SetSeqNoRound(2);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        fixture.Runtime.WaitFor("newer scan grant held", [&] { return grants.size() == 1; });
        fixture.Send(fixture.Shards[0]);
        const auto stale = fixture.Reply(NKikimrIndexBuilder::BAD_REQUEST);
        UNIT_ASSERT_VALUES_EQUAL(stale->Get()->Record.GetRequestSeqNoRound(), 1);
        UNIT_ASSERT(!stale->Get()->Record.HasRetryAfterMs());
        UNIT_ASSERT_STRING_CONTAINS(stale->Get()->Record.ShortDebugString(), "Stale index scan");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        grants.Stop().Unblock();
        const auto done = fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(done->Get()->Record.GetRequestSeqNoRound(), 2);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(QueuedScanKeepsRequestedMvccVersion) {
        TAdmissionFixture fixture;
        fixture.InsertRow("old");
        const auto snapshot = CreateVolatileSnapshot(fixture.Server, {"/Root/table-main"});
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        auto request = fixture.Request(fixture.Shards[0]);
        request->Record.SetSnapshotStep(snapshot.Step);
        request->Record.SetSnapshotTxId(snapshot.TxId);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        fixture.Runtime.WaitFor("scan version chosen", [&] { return grants.size() == 1; });
        fixture.InsertRow("new");
        grants.Stop().Unblock();
        fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_STRING_CONTAINS(ReadShardedTable(fixture.Server, "/Root/table-posting"), "data = old");
    }

    Y_UNIT_TEST(QueuedScanKeepsItsEstimatedRowLimit) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexRowBytesLimit(64);
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("scan row limit chosen", [&] { return grants.size() == 1; });
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexRowBytesLimit(8_MB);
        grants.Stop().Unblock();
        auto reply = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRequiredRowBytes(), 64);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRetryAfterMs(), 0);
    }

    Y_UNIT_TEST(RetryWaitsForPreviousUploadPayloadCleanup) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(fixture.Runtime);
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("first upload held", [&] { return uploads.size() == 1; });
        ui32 timeouts = 0;
        auto observer = fixture.Runtime.AddObserver<TEvTxUserProxy::TEvUploadRowsResponse>([&](auto& event) {
            if (event->Get()->Status == Ydb::StatusIds::TIMEOUT) {
                ++timeouts;
            }
        });
        // The upload actor times out at five minutes. Its protobuf remains
        // held here, so a retry must wait instead of retaining a second copy.
        fixture.Runtime.SimulateSleep(TDuration::Minutes(5) + TDuration::Seconds(15));
        UNIT_ASSERT_VALUES_EQUAL(timeouts, 1);
        UNIT_ASSERT_VALUES_EQUAL(uploads.size(), 1);
        uploads.Stop().Unblock();
        fixture.Reply(NKikimrIndexBuilder::DONE);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(RetainedUploadFailsAfterBoundedCleanupWait) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(fixture.Runtime);
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("upload retained beyond timeout", [&] { return uploads.size() == 1; });
        std::weak_ptr<void> reservation = (*uploads.begin())->Get()->MemoryReservation;
        fixture.Runtime.SimulateSleep(TDuration::Minutes(7));
        auto reply = fixture.Reply(NKikimrIndexBuilder::BUILD_ERROR);
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Record.ShortDebugString(), "previous upload payload cleanup");
        reply.Reset();
        UNIT_ASSERT_VALUES_EQUAL(uploads.size(), 1);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
        UNIT_ASSERT(!reservation.expired());
        uploads.Stop().Unblock();
        fixture.Runtime.WaitFor("retained payload releases broker reservation", [&] { return reservation.expired(); });
    }

    Y_UNIT_TEST(BlockedUploadKeepsReservationAfterSourceReboot) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(fixture.Runtime);
        fixture.Send(fixture.Shards[0]);
        fixture.Runtime.WaitFor("local upload held", [&] { return uploads.size() == 1; });
        std::weak_ptr<void> reservation = (*uploads.begin())->Get()->MemoryReservation;
        UNIT_ASSERT(!reservation.expired());
        RebootTablet(fixture.Runtime, fixture.Shards[0], fixture.Sender);
        auto aborted = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        aborted.Reset();
        fixture.Runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT(!reservation.expired());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
        uploads.Stop().Unblock();
        fixture.Runtime.WaitFor("upload storage and reservation released", [&] { return reservation.expired(); });
    }

    Y_UNIT_TEST(FilterKeyGroupCannotGrowWithoutLimit) {
        using namespace Tests;
        using namespace NTableIndex::NKMeans;
        TPortManager ports;
        TServer::TPtr server = new TServer(TServerSettings(ports.GetPort(2134)).SetDomainName("Root").SetUseRealThreads(false));
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        TShardedTableOptions options;
        options.Shards(1).AllowSystemColumnNames(true).Columns({
            {"key", "Uint32", true, true},
            {ParentColumn, ClusterIdTypeName, true, true},
            {DistanceColumn, "Double", false, true},
            {"data", "String", false, false},
        });
        CreateShardedTable(server, sender, "/Root", "table-main", options);
        CreatePostingTable(server, sender, options);
        ExecSQL(server, sender, R"(UPSERT INTO `/Root/table-main` (key, __ydb_parent, __ydb_distance, data) VALUES
            (1, 1, 0.1, 'one'), (1, 2, 0.1, 'one'), (1, 3, 0.1, 'one'), (1, 4, 0.1, 'one'),
            (1, 5, 0.1, 'one'), (1, 6, 0.1, 'one'), (1, 7, 0.1, 'one');)");
        const auto shard = GetTableShards(server, sender, "/Root/table-main").at(0);
        const auto table = ResolveTableId(server, sender, "/Root/table-main");
        auto event = MakeHolder<TEvDataShard::TEvFilterKMeansRequest>();
        auto& rec = event->Record;
        rec.SetAllowMemoryAdmission(true);
        rec.SetId(1);
        rec.SetSeqNoGeneration(1);
        rec.SetSeqNoRound(1);
        rec.SetTabletId(shard);
        table.PathId.ToProto(rec.MutablePathId());
        rec.SetUpload(NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING);
        rec.SetOutputName("/Root/table-posting");
        rec.SetDatabaseName("/Root");
        rec.SetOverlapClusters(2);
        rec.SetOverlapRatio(1.5);
        rec.MutableScanSettings()->SetMaxBatchRows(1);
        runtime.SendToPipe(shard, sender, event.Release(), 0, GetPipeConfigWithRetries());
        const auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvFilterKMeansResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::BUILD_ERROR, reply->Get()->Record.ShortDebugString());
        UNIT_ASSERT_STRING_CONTAINS(reply->Get()->Record.ShortDebugString(), "Filter key group exceeds");
    }

    Y_UNIT_TEST(SampleCoveringColumnsCannotExceedReservation) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        auto event = MakeHolder<TEvDataShard::TEvSampleKRequest>();
        auto& rec = event->Record;
        rec.SetAllowMemoryAdmission(true);
        rec.SetId(1);
        rec.SetSeqNoGeneration(1);
        rec.SetSeqNoRound(1);
        rec.SetTabletId(fixture.Shards[0]);
        fixture.Table.PathId.ToProto(rec.MutablePathId());
        rec.SetK(2);
        rec.SetMaxProbability(Max<ui64>());
        const TCell key = TCell::Make(ui32(1));
        TSerializedTableRange range({&key, 1}, true, {&key, 1}, true);
        range.Serialize(*rec.MutableKeyRange());
        rec.AddColumns("embedding");
        rec.AddColumns("data");
        rec.MutableSettings()->set_vector_dimension(2);
        rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        auto retry = MakeHolder<TEvDataShard::TEvSampleKRequest>();
        retry->Record.CopyFrom(rec);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, event.Release(), 0, GetPipeConfigWithRetries());
        const auto reply = fixture.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvSampleKResponse>(fixture.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::ABORTED, reply->Get()->Record.ShortDebugString());
        UNIT_ASSERT_GT(reply->Get()->Record.GetRequiredRowBytes(), 16_KB);
        retry->Record.SetRequiredRowBytes(reply->Get()->Record.GetRequiredRowBytes());
        retry->Record.SetSeqNoRound(2);
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, retry.Release(), 0, GetPipeConfigWithRetries());
        const auto done = fixture.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvSampleKResponse>(fixture.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(done->Get()->Record.GetStatus(), NKikimrIndexBuilder::DONE, done->Get()->Record.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(done->Get()->Record.RowsSize(), 1);
    }

    Y_UNIT_TEST(RowLimitStopsBufferGrowth) {
        TAdmissionFixture fixture;
        fixture.InsertRow();
        fixture.Runtime.GetAppData().DataShardConfig.SetBuildIndexRowBytesLimit(1);
        fixture.Send(fixture.Shards[0]);
        auto reply = fixture.Reply(NKikimrIndexBuilder::ABORTED);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRequiredRowBytes(), 16_KB);
        UNIT_ASSERT_GT(reply->Get()->Record.GetRetryAfterMs(), 0);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    Y_UNIT_TEST(RecomputeDoesNotConstructAccumulatorsBeforeGrant) {
        TAdmissionFixture fixture;
        using TGrant = NResourceBroker::TEvResourceBroker::TEvResourceAllocated;
        TBlockEvents<TGrant> grants(fixture.Runtime, [](const TGrant::TPtr& event) {
            auto* resource = dynamic_cast<NTabletFlatExecutor::TResource*>(event->Get()->Cookie.Get());
            return resource && resource->Source == NTabletFlatExecutor::TResource::ESource::Scan;
        });
        const auto before = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes").value();
        auto request = MakeHolder<TEvDataShard::TEvRecomputeKMeansRequest>();
        auto& rec = request->Record;
        rec.SetAllowMemoryAdmission(true);
        rec.SetId(1);
        rec.SetSeqNoGeneration(1);
        rec.SetSeqNoRound(1);
        rec.SetTabletId(fixture.Shards[0]);
        fixture.Table.PathId.ToProto(rec.MutablePathId());
        rec.MutableSettings()->set_vector_dimension(4096);
        rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
        rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
        rec.SetParent(0);
        rec.SetEmbeddingColumn("embedding");
        TString centroid(4096, 'a');
        centroid.push_back('\2');
        for (ui32 i = 0; i < 1024; ++i) {
            rec.AddClusters(centroid);
        }
        fixture.Runtime.SendToPipe(fixture.Shards[0], fixture.Sender, request.Release(), 0, GetPipeConfigWithRetries());
        fixture.Runtime.WaitFor("recompute grant held", [&] { return grants.size() == 1; });
        const auto queued = tcmalloc::MallocExtension::GetNumericProperty("generic.current_allocated_bytes").value();
        // The accumulator alone would be 32 MiB; only the 4 MiB protobuf
        // request and small executor state may remain before the grant.
        UNIT_ASSERT_C(queued < before + 20_MB, "Queued recompute allocated centroid accumulators");
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 1);
        grants.Stop().Unblock();
        auto reply = fixture.Runtime.GrabEdgeEventRethrow<TEvDataShard::TEvRecomputeKMeansResponse>(fixture.Sender);
        UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Record.GetStatus(), NKikimrIndexBuilder::DONE, reply->Get()->Record.ShortDebugString());
        UNIT_ASSERT_VALUES_EQUAL(fixture.Usage().Count, 0);
    }

    // Reproduces #56118 without exhausting the host: all scans are on one node,
    // and shard requests are held while each scan fills its next output batch.
    // Scale with vector-upload-scans and vector-upload-batch-bytes parameters.
    Y_UNIT_TEST(ConcurrentReshuffleWithBlockedUploads) {
        using namespace Tests;
        const ui32 scans = FromString<ui32>(GetTestParam("vector-upload-scans", "4"));
        const ui64 batchBytes = FromString<ui64>(GetTestParam("vector-upload-batch-bytes", ToString(8_MB)));
        // Stay below the default queue_build_index CPU limit of ten scans.
        UNIT_ASSERT(scans > 0 && scans <= 8);
        UNIT_ASSERT(batchBytes >= 64_KB && batchBytes <= 8_MB);
        constexpr ui32 payloadBytes = 16_KB;
        // Leave enough rows for two full batches and an unprocessed tail.
        const ui32 rowsPerShard = 2 * (batchBytes / payloadBytes + 1) + 32;

        THeapSamplingScope sampling;

        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root");
        settings.SetUseRealThreads(false);
        TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        CreateMainTable(server, sender, TShardedTableOptions().Shards(scans));
        CreatePostingTable(server, sender, TShardedTableOptions().Shards(1));
        const auto shards = GetTableShards(server, sender, "/Root/table-main");
        const auto tableId = ResolveTableId(server, sender, "/Root/table-main");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), scans);

        // Load through the real upload path, outside the profiled interval.
        // Uniform Uint32 partitioning puts exactly rowsPerShard rows per scan.
        {
            auto types = std::make_shared<NTxProxy::TUploadTypes>();
            for (const auto& [name, id] : TVector<std::pair<TString, Ydb::Type::PrimitiveTypeId>>{
                    {"key", Ydb::Type::UINT32}, {"embedding", Ydb::Type::STRING}, {"data", Ydb::Type::STRING}}) {
                Ydb::Type type;
                type.set_type_id(id);
                types->emplace_back(name, type);
            }
            const TString embedding("ab\2");
            const TString payload(payloadBytes, 'x');
            const TVector<TCell> values{TCell(embedding.data(), embedding.size()), TCell(payload.data(), payload.size())};
            for (ui32 shard = 0; shard < scans; ++shard) {
                auto rows = std::make_shared<NTxProxy::TUploadRows>();
                for (ui32 row = 0; row < rowsPerShard; ++row) {
                    const ui32 key = ((ui64(1) << 32) * shard + scans - 1) / scans + row;
                    const TVector<TCell> keys{TCell::Make(key)};
                    rows->emplace_back(TSerializedCellVec(keys), TSerializedCellVec::Serialize(values));
                }
                runtime.Register(NTxProxy::CreateUploadRowsInternal(
                    sender, "/Root", "/Root/table-main", types, std::move(rows)));
                const auto reply = runtime.GrabEdgeEventRethrow<TEvTxUserProxy::TEvUploadRowsResponse>(sender);
                UNIT_ASSERT_VALUES_EQUAL_C(reply->Get()->Status, Ydb::StatusIds::SUCCESS, reply->Get()->Issues.ToString());
            }
        }
        const auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-main"});
        runtime.SimulateSleep(TDuration::Seconds(1));
        DumpUploadHeap("baseline");

        const bool admission = GetTestParam("vector-upload-admission") == "true";
        ui32 scanSubmissions = 0;
        ui64 reservationBytes = 0;
        auto brokerObserver = runtime.AddObserver<NResourceBroker::TEvResourceBroker::TEvSubmitTask>([&](auto& event) {
            if (event->Get()->Task.Type == "build_index") {
                ++scanSubmissions;
                reservationBytes = event->Get()->Task.RequiredResources[NKikimrResourceBroker::MEMORY];
            }
        });
        TBlockEvents<TEvDataShard::TEvUploadRowsRequest> uploads(runtime);
        for (const ui64 shard : shards) {
            auto request = MakeHolder<TEvDataShard::TEvReshuffleKMeansRequest>();
            auto& rec = request->Record;
            rec.SetAllowMemoryAdmission(GetTestParam("vector-upload-admission") == "true");
            rec.SetId(1);
            rec.SetSeqNoGeneration(1);
            rec.SetSeqNoRound(1);
            rec.SetTabletId(shard);
            tableId.PathId.ToProto(rec.MutablePathId());
            rec.SetSnapshotTxId(snapshot.TxId);
            rec.SetSnapshotStep(snapshot.Step);
            rec.MutableSettings()->set_vector_dimension(2);
            rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
            rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            rec.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING);
            rec.SetParent(0);
            rec.SetChild(1);
            rec.AddClusters("ab\2");
            rec.SetEmbeddingColumn("embedding");
            rec.AddDataColumns("data");
            rec.SetDatabaseName("/Root");
            rec.SetOutputName("/Root/table-posting");
            rec.MutableScanSettings()->SetMaxBatchBytes(batchBytes);
            runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        runtime.WaitFor("all scan requests submitted", [&] { return scanSubmissions == scans; }, TDuration::Seconds(30));
        runtime.Send(new IEventHandle(NResourceBroker::MakeResourceBrokerID(), sender,
            new NResourceBroker::TEvResourceBroker::TEvConfigRequest("queue_build_index")));
        const auto brokerConfig = runtime.GrabEdgeEventRethrow<NResourceBroker::TEvResourceBroker::TEvConfigResponse>(sender);
        const ui64 activeMemoryLimit = brokerConfig->Get()->QueueConfig->GetLimit().GetMemory();
        const ui32 active = admission ? Min<ui64>(scans, activeMemoryLimit / reservationBytes) : scans;
        UNIT_ASSERT_GT(active, 0);
        runtime.WaitFor("one blocked upload per admitted scan", [&] { return uploads.size() >= active; }, TDuration::Seconds(30));
        // Let scans fill their second buffers and go to sleep on backpressure.
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(uploads.size(), active);
        THashSet<TActorId> uploaders;
        ui64 requestBytes = 0;
        for (const auto& ev : uploads) {
            UNIT_ASSERT(uploaders.insert(ev->Sender).second);
            ui64 bytes = 0;
            for (const auto& row : ev->Get()->Record.GetRows()) {
                bytes += row.GetKeyColumns().size() + row.GetValueColumns().size();
            }
            UNIT_ASSERT_GT(bytes, batchBytes);
            UNIT_ASSERT_LE(bytes, batchBytes + payloadBytes + 128);
            requestBytes += bytes;
        }
        Cerr << "VECTOR_UPLOAD_MEMORY scans=" << scans << " active=" << active << " reservation_bytes=" << reservationBytes << " batch_bytes=" << batchBytes
             << " rows_per_shard=" << rowsPerShard << " blocked_request_bytes=" << requestBytes << Endl;
        DumpUploadHeap("blocked");

        // More elapsed time cannot start another upload while the first is held.
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(uploads.size(), active);
        uploads.Stop().Unblock();
        ui64 readRows = 0;
        ui64 uploadRows = 0;
        THashSet<ui64> completed;
        while (completed.size() < scans) {
            const auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReshuffleKMeansResponse>(sender);
            const auto& rec = reply->Get()->Record;
            if (rec.GetStatus() == NKikimrIndexBuilder::IN_PROGRESS) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(rec.GetStatus(), NKikimrIndexBuilder::DONE, rec.ShortDebugString());
            UNIT_ASSERT(completed.insert(rec.GetTabletId()).second);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetMeteringStats().GetReadRows(), rowsPerShard);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetMeteringStats().GetUploadRows(), rowsPerShard);
            readRows += rec.GetMeteringStats().GetReadRows();
            uploadRows += rec.GetMeteringStats().GetUploadRows();
        }
        UNIT_ASSERT_VALUES_EQUAL(readRows, ui64(scans) * rowsPerShard);
        UNIT_ASSERT_VALUES_EQUAL(uploadRows, readRows);
        runtime.SimulateSleep(TDuration::Seconds(1));
        DumpUploadHeap("finished");
        UNIT_ASSERT_VALUES_EQUAL(NDataShard::NKqpHelpers::KqpSimpleExecSuccess(runtime,
            "SELECT COUNT(*) FROM `/Root/table-posting`;"),
            TStringBuilder() << "{ items { uint64_value: " << uploadRows << " } }");
    }

    Y_UNIT_TEST(ReshuffleCentroidsBeforeAdmission) {
        using namespace Tests;
        // Hold real scan submissions before resource-broker admission. The
        // empty source isolates centroid state from row data and upload buffers.
        const ui32 scans = FromString<ui32>(GetTestParam("vector-centroid-scans", "32"));
        const ui32 dimensions = FromString<ui32>(GetTestParam("vector-centroid-dimensions", "1536"));
        const ui32 count = FromString<ui32>(GetTestParam("vector-centroid-clusters", "256"));
        const bool distinctPayloads = GetTestParam("vector-centroid-distinct-payloads") == "true";
        const bool admission = GetTestParam("vector-upload-admission") == "true";
        UNIT_ASSERT(scans > 0 && scans <= (admission ? 128 : 32));
        UNIT_ASSERT(dimensions > 0 && dimensions <= 4096);
        UNIT_ASSERT(count > 0 && count <= 1024);
        THeapSamplingScope sampling;

        TPortManager pm;
        TServerSettings settings(pm.GetPort(2134));
        settings.SetDomainName("Root");
        settings.SetUseRealThreads(false);
        TServer::TPtr server = new TServer(settings);
        auto& runtime = *server->GetRuntime();
        const auto sender = runtime.AllocateEdgeActor();
        InitRoot(server, sender);
        CreateMainTable(server, sender, TShardedTableOptions().Shards(scans));
        CreatePostingTable(server, sender, TShardedTableOptions().Shards(1));
        const auto shards = GetTableShards(server, sender, "/Root/table-main");
        const auto tableId = ResolveTableId(server, sender, "/Root/table-main");
        const auto snapshot = CreateVolatileSnapshot(server, {"/Root/table-main"});
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), scans);
        TString centroid(dimensions, 'a');
        centroid.push_back('\2');
        runtime.SimulateSleep(TDuration::Seconds(1));
        DumpUploadHeap("centroids-baseline");

        ui32 uploadRequests = 0;
        THashSet<ui64> rejected;
        auto rejectionObserver = runtime.AddObserver<TEvDataShard::TEvReshuffleKMeansResponse>([&](auto& event) {
            const auto& rec = event->Get()->Record;
            if (rec.GetStatus() == NKikimrIndexBuilder::ABORTED && rec.GetRetryAfterMs()) {
                rejected.insert(rec.GetTabletId());
            }
        });
        auto uploadObserver = runtime.AddObserver<TEvDataShard::TEvUploadRowsRequest>(
            [&](auto&) { ++uploadRequests; });
        using TSubmit = NResourceBroker::TEvResourceBroker::TEvSubmitTask;
        TBlockEvents<TSubmit> submissions(runtime, [](const TSubmit::TPtr& ev) {
            return ev->Get()->Task.Type == "build_index";
        });
        for (const ui64 shard : shards) {
            auto request = MakeHolder<TEvDataShard::TEvReshuffleKMeansRequest>();
            auto& rec = request->Record;
            rec.SetAllowMemoryAdmission(GetTestParam("vector-upload-admission") == "true");
            rec.SetId(1);
            rec.SetSeqNoGeneration(1);
            rec.SetSeqNoRound(1);
            rec.SetTabletId(shard);
            tableId.PathId.ToProto(rec.MutablePathId());
            rec.SetSnapshotTxId(snapshot.TxId);
            rec.SetSnapshotStep(snapshot.Step);
            rec.MutableSettings()->set_vector_dimension(dimensions);
            rec.MutableSettings()->set_vector_type(Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8);
            rec.MutableSettings()->set_metric(Ydb::Table::VectorIndexSettings::DISTANCE_COSINE);
            rec.SetUpload(NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING);
            rec.SetParent(0);
            rec.SetChild(1);
            for (ui32 i = 0; i < count; ++i) {
                if (distinctPayloads) {
                    // Real network requests own their payload allocations. The
                    // historical in-process case shares TString backing storage.
                    rec.AddClusters(TString(centroid.data(), centroid.size()));
                } else {
                    rec.AddClusters(centroid);
                }
            }
            rec.SetEmbeddingColumn("embedding");
            rec.AddDataColumns("data");
            rec.SetDatabaseName("/Root");
            rec.SetOutputName("/Root/table-posting");
            runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());
        }
        runtime.WaitFor("all scans waiting for resource admission",
            [&] { return submissions.size() + rejected.size() >= scans; }, TDuration::Seconds(30));
        runtime.SimulateSleep(TDuration::Seconds(1));
        UNIT_ASSERT_VALUES_EQUAL(submissions.size() + rejected.size(), scans);
        UNIT_ASSERT_VALUES_EQUAL(uploadRequests, 0);
        if (admission) {
            const auto usage = runtime.GetAppData().BuildIndexScanAdmission->GetUsage();
            UNIT_ASSERT_VALUES_EQUAL(usage.Count, submissions.size());
            UNIT_ASSERT_LE(usage.Bytes, 64_MB);
            UNIT_ASSERT_LE(usage.Count, 128);
        }
        ui64 requestedMemory = 0;
        for (const auto& ev : submissions) {
            requestedMemory += ev->Get()->Task.RequiredResources[NKikimrResourceBroker::MEMORY];
        }
        Cerr << "VECTOR_CENTROID_MEMORY scans=" << scans << " dimensions=" << dimensions
             << " clusters=" << count << " admitted=" << submissions.size() << " rejected=" << rejected.size()
             << " requested_memory=" << requestedMemory
             << " pending_bytes=" << runtime.GetAppData().BuildIndexScanAdmission->GetUsage().Bytes
             << " distinct_payloads=" << distinctPayloads << Endl;
        DumpUploadHeap("centroids-blocked");
        submissions.Stop().Unblock();
        THashSet<ui64> completed = rejected;
        while (completed.size() < scans) {
            const auto reply = runtime.GrabEdgeEventRethrow<TEvDataShard::TEvReshuffleKMeansResponse>(sender);
            const auto& rec = reply->Get()->Record;
            if (rec.GetStatus() == NKikimrIndexBuilder::ABORTED && rec.GetRetryAfterMs()) {
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(rec.GetStatus(), NKikimrIndexBuilder::DONE, rec.ShortDebugString());
            UNIT_ASSERT(completed.insert(rec.GetTabletId()).second);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetMeteringStats().GetReadRows(), 0);
            UNIT_ASSERT_VALUES_EQUAL(rec.GetMeteringStats().GetUploadRows(), 0);
        }
        UNIT_ASSERT_VALUES_EQUAL(uploadRequests, 0);
        runtime.SimulateSleep(TDuration::Seconds(1));
        DumpUploadHeap("centroids-finished");
    }
}

} // namespace NKikimr
