#include "datashard_impl.h"
#include "kmeans_helper.h"
#include "scan_common.h"
#include "upload_stats.h"
#include "buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_tablecell.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <yql/essentials/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {
using namespace NKMeans;

class TResult {
public:
    explicit TResult(const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvLocalKMeansResponse> response)
        : ResponseActorId{responseActorId}
        , Response{std::move(response)} {
            Y_ASSERT(Response);
        }

    void SizeAdd(i64 size) {
        if (size != 0) {
            std::lock_guard lock{Mutex};
            Size += size;
        }
    }

    template<typename Func>
    void Send(const TActorContext& ctx, Func&& func) {
        std::unique_lock lock{Mutex};
        if (Size <= 0) {
            return;
        }
        if (func(Response->Record) && --Size > 0) {
            return;
        }
        Size = 0;
        lock.unlock();
        ctx.Send(ResponseActorId, std::move(Response));
    }

private:
    std::mutex Mutex;
    i64 Size = 1;
    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvLocalKMeansResponse> Response;
};

// This scan needed to run local (not distributed) kmeans.
// We have this local stage because we construct kmeans tree from top to bottom.
// And bottom kmeans can be constructed completely locally in datashards to avoid extra communication.
// Also it can be used for small tables.
//
// This class is kind of state machine, it has 3 phases.
// Each of them corresponds to 1-N rounds of NTable::IScan (which is kind of state machine itself).
// 1. First iteration collect sample of clusters
// 2. Then N iterations recompute clusters (main cycle of batched kmeans)
// 3. Finally last iteration upload clusters to level table and postings to corresponding posting table
//
// These phases maps to State:
// 1. -- EState::SAMPLE
// 2. -- EState::KMEANS
// 3. -- EState::UPLOAD*
//
// Which UPLOAD* will be used depends on that will client of this scan request (see UploadState)
//
// NTable::IScan::Seek used to switch from current state to the next one.

// If less than 1% of vectors are reassigned to new clusters we want to stop
// TODO(mbkkt) 1% is choosed by common sense and should be adjusted in future
static constexpr double MinVectorsNeedsReassigned = 0.01;

class TLocalKMeansScanBase: public TActor<TLocalKMeansScanBase>, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::TEvLocalKMeansRequest;

    NTableIndex::TClusterId Parent = 0;
    NTableIndex::TClusterId Child = 0;

    ui32 Round = 0;
    ui32 MaxRounds = 0;

    ui32 K = 0;

    EState::EState State;
    EState::EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    // Sample
    ui64 MaxProbability = std::numeric_limits<ui64>::max();
    TReallyFastRng32 Rng;

    struct TProbability {
        ui64 P = 0;
        ui64 I = 0;

        bool operator==(const TProbability&) const noexcept = default;
        auto operator<=>(const TProbability&) const noexcept = default;
    };

    std::vector<TProbability> MaxRows;
    std::vector<TString> Clusters;
    std::vector<ui64> ClusterSizes;

    // Upload
    std::shared_ptr<NTxProxy::TUploadTypes> TargetTypes;
    std::shared_ptr<NTxProxy::TUploadTypes> NextTypes;

    TString TargetTable;
    TString NextTable;

    TBufferData ReadBuf;
    TBufferData WriteBuf;

    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    ui32 RetryCount = 0;

    TActorId Uploader;
    TUploadLimits Limits;

    NTable::TTag KMeansScan;
    TTags UploadScan;

    TUploadStatus UploadStatus;

    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;

    std::shared_ptr<TResult> Result;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LOCAL_KMEANS_SCAN_ACTOR;
    }

    TLocalKMeansScanBase(ui64 buildId, const TUserTable& table, TLead&& lead, NTableIndex::TClusterId parent, NTableIndex::TClusterId child,
                         const NKikimrTxDataShard::TEvLocalKMeansRequest& request,
                         std::shared_ptr<TResult> result)
        : TActor{&TThis::StateWork}
        , Parent{parent}
        , Child{child}
        , MaxRounds{request.GetNeedsRounds() - request.GetDoneRounds()}
        , K{request.GetK()}
        , State{request.GetState()}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , BuildId{buildId}
        , Rng{request.GetSeed()}
        , TargetTable{request.GetLevelName()}
        , NextTable{request.GetPostingName()}
        , Result{std::move(result)}
    {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        UploadScan = MakeUploadTags(table, embedding, data, EmbeddingPos, DataPos, KMeansScan);
        // upload types
        if (Ydb::Type type; State <= EState::KMEANS) {
            TargetTypes = std::make_shared<NTxProxy::TUploadTypes>(3);
            type.set_type_id(NTableIndex::ClusterIdType);
            (*TargetTypes)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, type};
            (*TargetTypes)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type};
            type.set_type_id(Ydb::Type::STRING);
            (*TargetTypes)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn, type};
        }
        NextTypes = MakeUploadTypes(table, UploadState, embedding, data);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_D("Prepare " << Debug());

        Driver = driver;
        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) final
    {
        LOG_D("Finish " << Debug());

        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoisonPill);
            Uploader = {};
        }

        Result->Send(TActivationContext::AsActorContext(), [&] (NKikimrTxDataShard::TEvLocalKMeansResponse& response) {
            response.SetReadRows(ReadRows);
            response.SetReadBytes(ReadBytes);
            response.SetUploadRows(UploadRows);
            response.SetUploadBytes(UploadBytes);
            NYql::IssuesToMessage(UploadStatus.Issues, response.MutableIssues());
            if (abort != EAbort::None) {
                response.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
                return false;
            } else if (UploadStatus.IsSuccess()) {
                response.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
                return true;
            } else {
                response.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
                return false;
            }
        });

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const final
    {
        out << Debug();
    }

    TString Debug() const
    {
        return TStringBuilder() << " TLocalKMeansScan Id: " << BuildId << " Parent: " << Parent << " Child: " << Child
            << " Target: " << TargetTable << " K: " << K << " Clusters: " << Clusters.size()
            << " State: " << State << " Round: " << Round << " / " << MaxRounds
            << " ReadBuf size: " << ReadBuf.Size() << " WriteBuf size: " << WriteBuf.Size() << " ";
    }

    EScan PageFault() final
    {
        LOG_T("PageFault " << Debug());

        if (!ReadBuf.IsEmpty() && WriteBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload(false);
        }

        return EScan::Feed;
    }

protected:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("TLocalKMeansScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: "
                                                                            << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/)
    {
        LOG_T("Retry upload " << Debug());

        if (!WriteBuf.IsEmpty()) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        LOG_D("Handle TEvUploadRowsResponse " << Debug()
            << " Uploader: " << Uploader.ToString() << " ev->Sender: " << ev->Sender.ToString());

        if (Uploader) {
            Y_VERIFY_S(Uploader == ev->Sender, "Mismatch Uploader: " << Uploader.ToString() << " ev->Sender: "
                                                                     << ev->Sender.ToString() << Debug());
        } else {
            Y_ABORT_UNLESS(Driver == nullptr);
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = ev->Get()->Issues;
        if (UploadStatus.IsSuccess()) {
            UploadRows += WriteBuf.GetRows();
            UploadBytes += WriteBuf.GetBytes();
            WriteBuf.Clear();
            if (!ReadBuf.IsEmpty() && ReadBuf.IsReachLimits(Limits)) {
                ReadBuf.FlushTo(WriteBuf);
                Upload(false);
            }

            Driver->Touch(EScan::Feed);
            return;
        }

        if (RetryCount < Limits.MaxUploadRowsRetryCount && UploadStatus.IsRetriable()) {
            LOG_N("Got retriable error, " << Debug() << UploadStatus.ToString());

            ctx.Schedule(Limits.GetTimeoutBackouff(RetryCount), new TEvents::TEvWakeup());
            return;
        }

        LOG_N("Got error, abort scan, " << Debug() << UploadStatus.ToString());

        Driver->Touch(EScan::Final);
    }

    EScan FeedUpload()
    {
        if (!ReadBuf.IsReachLimits(Limits)) {
            return EScan::Feed;
        }
        if (!WriteBuf.IsEmpty()) {
            return EScan::Sleep;
        }
        ReadBuf.FlushTo(WriteBuf);
        Upload(false);
        return EScan::Feed;
    }

    ui64 GetProbability()
    {
        return Rng.GenRand64();
    }

    void Upload(bool isRetry)
    {
        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
            if (State != EState::KMEANS && NextTypes) {
                TargetTypes = std::exchange(NextTypes, {});
                TargetTable = std::move(NextTable);
            }
        }

        auto actor = NTxProxy::CreateUploadRowsInternal(
            this->SelfId(), TargetTable, TargetTypes, WriteBuf.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow, true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }

    void UploadSample()
    {
        Y_ASSERT(ReadBuf.IsEmpty());
        Y_ASSERT(WriteBuf.IsEmpty());
        std::array<TCell, 2> pk;
        std::array<TCell, 1> data;
        for (NTable::TPos pos = 0; const auto& row : Clusters) {
            pk[0] = TCell::Make(Parent);
            pk[1] = TCell::Make(Child + pos);
            data[0] = TCell{row};
            WriteBuf.AddRow({}, TSerializedCellVec{pk}, TSerializedCellVec::Serialize(data));
            ++pos;
        }
        Upload(false);
    }
};

template <typename TMetric>
class TLocalKMeansScan final: public TLocalKMeansScanBase, private TCalculation<TMetric> {
    // KMeans
    using TEmbedding = std::vector<typename TMetric::TSum>;

    struct TAggregatedCluster {
        TEmbedding Cluster;
        ui64 Size = 0;
    };
    std::vector<TAggregatedCluster> AggregatedClusters;

public:
    TLocalKMeansScan(ui64 buildId, const TUserTable& table, TLead&& lead, NTableIndex::TClusterId parent, NTableIndex::TClusterId child, NKikimrTxDataShard::TEvLocalKMeansRequest& request,
                     std::shared_ptr<TResult> result)
        : TLocalKMeansScanBase{buildId, table, std::move(lead), parent, child, request, std::move(result)}
    {
        this->Dimensions = request.GetSettings().vector_dimension();
        LOG_D("Create " << Debug());
    }

    EScan Seek(TLead& lead, ui64 seq) final
    {
        LOG_D("Seek " << Debug());
        if (State == UploadState) {
            if (!WriteBuf.IsEmpty()) {
                return EScan::Sleep;
            }
            if (!ReadBuf.IsEmpty()) {
                ReadBuf.FlushTo(WriteBuf);
                Upload(false);
                return EScan::Sleep;
            }
            if (UploadStatus.IsNone()) {
                UploadStatus.StatusCode = Ydb::StatusIds::SUCCESS;
            }
            return EScan::Final;
        }

        if (State == EState::SAMPLE) {
            lead = Lead;
            lead.SetTags({&KMeansScan, 1});
            if (seq == 0) {
                return EScan::Feed;
            }
            State = EState::KMEANS;
            if (!InitAggregatedClusters()) {
                // We don't need to do anything,
                // because this datashard doesn't have valid embeddings for this parent
                if (UploadStatus.IsNone()) {
                    UploadStatus.StatusCode = Ydb::StatusIds::SUCCESS;
                }
                return EScan::Final;
            }
            ++Round;
            return EScan::Feed;
        }

        Y_ASSERT(State == EState::KMEANS);
        if (RecomputeClusters()) {
            lead = std::move(Lead);
            lead.SetTags(UploadScan);

            UploadSample();
            State = UploadState;
        } else {
            lead = Lead;
            lead.SetTags({&KMeansScan, 1});
            ++Round;
        }
        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        LOG_T("Feed " << Debug());
        ++ReadRows;
        ReadBytes += CountBytes(key, row);
        switch (State) {
            case EState::SAMPLE:
                return FeedSample(row);
            case EState::KMEANS:
                return FeedKMeans(row);
            case EState::UPLOAD_MAIN_TO_BUILD:
                return FeedUploadMain2Build(key, row);
            case EState::UPLOAD_MAIN_TO_POSTING:
                return FeedUploadMain2Posting(key, row);
            case EState::UPLOAD_BUILD_TO_BUILD:
                return FeedUploadBuild2Build(key, row);
            case EState::UPLOAD_BUILD_TO_POSTING:
                return FeedUploadBuild2Posting(key, row);
            default:
                return EScan::Final;
        }
    }

private:
    bool InitAggregatedClusters()
    {
        if (Clusters.size() == 0) {
            return false;
        }
        if (Clusters.size() < K) {
            // if this datashard have smaller than K count of valid embeddings for this parent
            // lets make single centroid for it
            K = 1;
            Clusters.resize(K);
        }
        Y_ASSERT(Clusters.size() == K);
        ClusterSizes.resize(K, 0);
        AggregatedClusters.resize(K);
        for (auto& aggregate : AggregatedClusters) {
            aggregate.Cluster.resize(this->Dimensions, 0);
        }
        return true;
    }

    void AggregateToCluster(ui32 pos, const char* embedding)
    {
        if (pos >= K) {
            return;
        }
        auto& aggregate = AggregatedClusters[pos];
        auto* coords = aggregate.Cluster.data();
        for (auto coord : this->GetCoords(embedding)) {
            *coords++ += coord;
        }
        ++aggregate.Size;
    }

    bool RecomputeClusters()
    {
        Y_ASSERT(K >= 1);
        ui64 vectorCount = 0;
        ui64 reassignedCount = 0;
        for (size_t i = 0; auto& aggregate : AggregatedClusters) {
            vectorCount += aggregate.Size;

            auto& clusterSize = ClusterSizes[i];
            reassignedCount += clusterSize < aggregate.Size ? aggregate.Size - clusterSize : 0;
            clusterSize = aggregate.Size;

            if (aggregate.Size != 0) {
                this->Fill(Clusters[i], aggregate.Cluster.data(), aggregate.Size);
                Y_ASSERT(aggregate.Size == 0);
            }
            ++i;
        }
        Y_ASSERT(vectorCount >= K);
        Y_ASSERT(reassignedCount <= vectorCount);
        if (K == 1) {
            return true;
        }

        bool last = Round >= MaxRounds;
        if (!last && Round > 1) {
            const auto changes = static_cast<double>(reassignedCount) / static_cast<double>(vectorCount);
            last = changes < MinVectorsNeedsReassigned;
        }
        if (!last) {
            return false;
        }

        size_t w = 0;
        for (size_t r = 0; r < ClusterSizes.size(); ++r) {
            if (ClusterSizes[r] != 0) {
                ClusterSizes[w] = ClusterSizes[r];
                Clusters[w] = std::move(Clusters[r]);
                ++w;
            }
        }
        ClusterSizes.erase(ClusterSizes.begin() + w, ClusterSizes.end());
        Clusters.erase(Clusters.begin() + w, Clusters.end());
        return true;
    }

    EScan FeedSample(const TRow& row)
    {
        Y_ASSERT(row.Size() == 1);
        const auto embedding = row.Get(0).AsRef();
        if (!this->IsExpectedSize(embedding)) {
            return EScan::Feed;
        }

        const auto probability = GetProbability();
        if (Clusters.size() < K) {
            MaxRows.push_back({probability, Clusters.size()});
            Clusters.emplace_back(embedding.data(), embedding.size());
            if (Clusters.size() == K) {
                std::make_heap(MaxRows.begin(), MaxRows.end());
                MaxProbability = MaxRows.front().P;
            }
        } else if (probability < MaxProbability) {
            // TODO(mbkkt) use tournament tree to make less compare and swaps
            std::pop_heap(MaxRows.begin(), MaxRows.end());
            Clusters[MaxRows.back().I].assign(embedding.data(), embedding.size());
            MaxRows.back().P = probability;
            std::push_heap(MaxRows.begin(), MaxRows.end());
            MaxProbability = MaxRows.front().P;
        }
        return MaxProbability != 0 ? EScan::Feed : EScan::Reset;
    }

    EScan FeedKMeans(const TRow& row)
    {
        Y_ASSERT(row.Size() == 1);
        const ui32 pos = FeedEmbedding(*this, Clusters, row, 0);
        AggregateToCluster(pos, row.Get(0).Data());
        return EScan::Feed;
    }

    EScan FeedUploadMain2Build(TArrayRef<const TCell> key, const TRow& row)
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowMain2Build(ReadBuf, Child + pos, key, row);
        return FeedUpload();
    }

    EScan FeedUploadMain2Posting(TArrayRef<const TCell> key, const TRow& row)
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowMain2Posting(ReadBuf, Child + pos, key, row, DataPos);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Build(TArrayRef<const TCell> key, const TRow& row)
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowBuild2Build(ReadBuf, Child + pos, key, row);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Posting(TArrayRef<const TCell> key, const TRow& row)
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowBuild2Posting(ReadBuf, Child + pos, key, row, DataPos);
        return FeedUpload();
    }
};

class TDataShard::TTxHandleSafeLocalKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeLocalKMeansScan(TDataShard* self, TEvDataShard::TEvLocalKMeansRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev))
    {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) final
    {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) final
    {
    }

private:
    TEvDataShard::TEvLocalKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvLocalKMeansRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeLocalKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvLocalKMeansRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const bool needsSnapshot = request.HasSnapshotStep() || request.HasSnapshotTxId();
    TRowVersion rowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId());
    if (!needsSnapshot) {
        rowVersion = GetMvccTxVersion(EMvccTxMode::ReadOnly);
    }

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }
    const ui64 id = request.GetId();

    auto response = MakeHolder<TEvDataShard::TEvLocalKMeansResponse>();
    response->Record.SetId(id);
    response->Record.SetTabletId(TabletID());

    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);
    auto result = std::make_shared<TResult>(ev->Sender, std::move(response));
    ui32 localTid = 0;
    TScanRecord::TScanIds scanIds;

    auto badRequest = [&](const TString& error) {
        for (auto scanId : scanIds) {
            CancelScan(localTid, scanId);
        }
        result->Send(ctx, [&] (NKikimrTxDataShard::TEvLocalKMeansResponse& response) {
            response.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
            auto issue = response.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
            return false;
        });
        result.reset();
    };

    if (const ui64 shardId = request.GetTabletId(); shardId != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
        return;
    }

    const auto pathId = TPathId::FromProto(request.GetPathId());
    const auto* userTableIt = GetUserTables().FindPtr(pathId.LocalPathId);
    if (!userTableIt) {
        badRequest(TStringBuilder() << "Unknown table id: " << pathId.LocalPathId);
        return;
    }
    Y_ABORT_UNLESS(*userTableIt);
    const auto& userTable = **userTableIt;

    if (const auto* recCard = ScanManager.Get(id)) {
        if (recCard->SeqNo == seqNo) {
            // do no start one more scan
            return;
        }

        for (auto scanId : recCard->ScanIds) {
            CancelScan(userTable.LocalTid, scanId);
        }
        ScanManager.Drop(id);
    }

    localTid = userTable.LocalTid;

    const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
    if (needsSnapshot && !SnapshotManager.FindAvailable(snapshotKey)) {
        badRequest(TStringBuilder() << "no snapshot has been found" << " , path id is " << pathId.OwnerId << ":"
                                    << pathId.LocalPathId << " , snapshot step is " << snapshotKey.Step
                                    << " , snapshot tx is " << snapshotKey.TxId);
        return;
    }

    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    if (request.GetK() < 2) {
        badRequest("Should be requested partition on at least two rows");
        return;
    }

    const auto parentFrom = request.GetParentFrom();
    const auto parentTo = request.GetParentTo();
    if (parentFrom > parentTo) {
        badRequest(TStringBuilder() << "Parent from " << parentFrom << " should be less or equal to parent to " << parentTo);
        return;
    }
    const i64 expectedSize = parentTo - parentFrom + 1;
    result->SizeAdd(expectedSize);

    for (auto parent = parentFrom; parent <= parentTo; ++parent) {
        TCell from, to;
        const auto range = CreateRangeFrom(userTable, parent, from, to);
        if (range.IsEmptyRange(userTable.KeyColumnTypes)) {
            LOG_D("TEvLocalKMeansRequst " << request.GetId() << " parent " << parent << " is empty");
            continue;
        }

        TAutoPtr<NTable::IScan> scan;
        auto createScan = [&]<typename T> {
            scan = new TLocalKMeansScan<T>{
                request.GetId(), userTable,
                CreateLeadFrom(range), parent, request.GetChild() + request.GetK() * (parent - parentFrom),
                request, result,
            };
        };
        MakeScan(request, createScan, badRequest);
        if (!scan) {
            Y_ASSERT(!result);
            return;
        }

        TScanOptions scanOpts;
        scanOpts.SetSnapshotRowVersion(rowVersion);
        scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?
        const auto scanId = QueueScan(userTable.LocalTid, std::move(scan), 0, scanOpts);
        scanIds.push_back(scanId);
    }

    if (scanIds.empty()) {
        badRequest("Requested range doesn't intersect with table range");
        return;
    }
    result->SizeAdd(static_cast<i64>(scanIds.size()) - expectedSize);
    result->Send(ctx, [] (auto&) { return true; });  // decrement extra one
    ScanManager.Set(id, seqNo) = std::move(scanIds);
}

}
