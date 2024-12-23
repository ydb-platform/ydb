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

    ui32 Parent = 0;
    ui32 Child = 0;

    ui32 Round = 0;
    ui32 MaxRounds = 0;

    ui32 K = 0;

    EState::EState State;
    EState::EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

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

    // Response
    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvLocalKMeansResponse> Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LOCAL_KMEANS_SCAN_ACTOR;
    }

    TLocalKMeansScanBase(const TUserTable& table, TLead&& lead,
                         const NKikimrTxDataShard::TEvLocalKMeansRequest& request, const TActorId& responseActorId,
                         TAutoPtr<TEvDataShard::TEvLocalKMeansResponse>&& response)
        : TActor{&TThis::StateWork}
        , Parent{request.GetParent()}
        , Child{request.GetChild()}
        , MaxRounds{request.GetNeedsRounds() - request.GetDoneRounds()}
        , K{request.GetK()}
        , State{request.GetState()}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , Rng{request.GetSeed()}
        , TargetTable{request.GetLevelName()}
        , NextTable{request.GetPostingName()}
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
    {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        UploadScan = MakeUploadTags(table, embedding, data, EmbeddingPos, DataPos, KMeansScan);
        // upload types
        if (Ydb::Type type; State <= EState::KMEANS) {
            TargetTypes = std::make_shared<NTxProxy::TUploadTypes>(3);
            type.set_type_id(Ydb::Type::UINT32);
            (*TargetTypes)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, type};
            (*TargetTypes)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type};
            type.set_type_id(Ydb::Type::STRING);
            (*TargetTypes)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn, type};
        }
        NextTypes = MakeUploadTypes(table, UploadState, embedding, data);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_T("Prepare " << Debug());

        Driver = driver;
        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        LOG_T("Finish " << Debug());

        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoisonPill);
            Uploader = {};
        }

        auto& record = Response->Record;
        record.SetReadRows(ReadRows);
        record.SetReadBytes(ReadBytes);
        record.SetUploadRows(UploadRows);
        record.SetUploadBytes(UploadBytes);
        if (abort != EAbort::None) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else if (UploadStatus.IsSuccess()) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
        } else {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        }
        NYql::IssuesToMessage(UploadStatus.Issues, record.MutableIssues());
        Send(ResponseActorId, Response.Release());

        Driver = nullptr;
        this->PassAway();
        return nullptr;
    }

    void Describe(IOutputStream& out) const noexcept final
    {
        out << Debug();
    }

    TString Debug() const
    {
        auto builder = TStringBuilder() << " TLocalKMeansScan";
        if (Response) {
            auto& r = Response->Record;
            builder << " Id: " << r.GetId();
        }
        return builder << " State: " << State << " Round: " << Round << " MaxRounds: " << MaxRounds
                       << " ReadBuf size: " << ReadBuf.Size() << " WriteBuf size: " << WriteBuf.Size() << " ";
    }

    EScan PageFault() noexcept final
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
        LOG_T("Handle TEvUploadRowsResponse " << Debug() << " Uploader: " << Uploader.ToString()
                                              << " ev->Sender: " << ev->Sender.ToString());

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
    TLocalKMeansScan(const TUserTable& table, TLead&& lead, NKikimrTxDataShard::TEvLocalKMeansRequest& request,
                     const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvLocalKMeansResponse>&& response)
        : TLocalKMeansScanBase{table, std::move(lead), request, responseActorId, std::move(response)}
    {
        this->Dimensions = request.GetSettings().vector_dimension();
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
        LOG_T("Seek " << Debug());
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

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final
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

    EScan FeedSample(const TRow& row) noexcept
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

    EScan FeedKMeans(const TRow& row) noexcept
    {
        Y_ASSERT(row.Size() == 1);
        const ui32 pos = FeedEmbedding(*this, Clusters, row, 0);
        AggregateToCluster(pos, row.Get(0).Data());
        return EScan::Feed;
    }

    EScan FeedUploadMain2Build(TArrayRef<const TCell> key, const TRow& row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowMain2Build(ReadBuf, Child + pos, key, row);
        return FeedUpload();
    }

    EScan FeedUploadMain2Posting(TArrayRef<const TCell> key, const TRow& row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowMain2Posting(ReadBuf, Child + pos, key, row, DataPos);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Build(TArrayRef<const TCell> key, const TRow& row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowBuild2Build(ReadBuf, Child + pos, key, row);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Posting(TArrayRef<const TCell> key, const TRow& row) noexcept
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
    auto& record = ev->Get()->Record;
    const bool needsSnapshot = record.HasSnapshotStep() || record.HasSnapshotTxId();
    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());
    if (!needsSnapshot) {
        rowVersion = GetMvccTxVersion(EMvccTxMode::ReadOnly);
    }

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }
    const ui64 id = record.GetId();

    auto response = MakeHolder<TEvDataShard::TEvLocalKMeansResponse>();
    response->Record.SetId(id);
    response->Record.SetTabletId(TabletID());

    TScanRecord::TSeqNo seqNo = {record.GetSeqNoGeneration(), record.GetSeqNoRound()};
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    auto badRequest = [&](const TString& error) {
        response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
        auto issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
        ctx.Send(ev->Sender, std::move(response));
    };

    if (const ui64 shardId = record.GetTabletId(); shardId != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << shardId << " this is " << TabletID());
        return;
    }

    const auto pathId = TPathId::FromProto(record.GetPathId());
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

        CancelScan(userTable.LocalTid, recCard->ScanId);
        ScanManager.Drop(id);
    }

    TCell from, to;
    const auto range = CreateRangeFrom(userTable, record.GetParent(), from, to);
    if (range.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range");
        return;
    }

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

    if (record.GetK() < 2) {
        badRequest(TStringBuilder() << "Should be requested partition on at least two rows");
        return;
    }

    TAutoPtr<NTable::IScan> scan;
    auto createScan = [&]<typename T> {
        scan = new TLocalKMeansScan<T>{
            userTable, CreateLeadFrom(range), record, ev->Sender, std::move(response),
        };
    };
    MakeScan(record, createScan, badRequest);
    if (!scan) {
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?
    const auto scanId = QueueScan(userTable.LocalTid, std::move(scan), 0, scanOpts);
    TScanRecord recCard = {scanId, seqNo};
    ScanManager.Set(id, recCard);
}

}
