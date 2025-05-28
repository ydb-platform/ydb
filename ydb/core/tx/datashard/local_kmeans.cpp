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
// TODO(mbkkt) 1% is choosen by common sense and should be adjusted in future
static constexpr double MinVectorsNeedsReassigned = 0.01;

class TLocalKMeansScanBase: public TActor<TLocalKMeansScanBase>, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::EKMeansState;

    NTableIndex::TClusterId Parent = 0;
    NTableIndex::TClusterId Child = 0;

    ui32 Round = 0;
    const ui32 MaxRounds = 0;

    const ui32 InitK = 0;
    ui32 K = 0;

    EState State;
    const EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    // Sample
    ui64 MaxProbability = std::numeric_limits<ui64>::max();
    TReallyFastRng32 Rng;

    struct TProbability {
        ui64 P = 0;
        ui64 I = 0;

        auto operator<=>(const TProbability&) const noexcept = default;
    };

    std::vector<TProbability> MaxRows;
    std::vector<TString> Clusters;
    std::vector<ui64> ClusterSizes;

    // Upload
    std::shared_ptr<NTxProxy::TUploadTypes> LevelTypes;
    std::shared_ptr<NTxProxy::TUploadTypes> PostingTypes;
    std::shared_ptr<NTxProxy::TUploadTypes> UploadTypes;

    const TString LevelTable;
    const TString PostingTable;
    TString UploadTable;

    TBufferData LevelBuf;
    TBufferData PostingBuf;
    TBufferData UploadBuf;

    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    ui32 RetryCount = 0;

    TActorId Uploader;
    const TIndexBuildScanSettings ScanSettings;

    NTable::TTag EmbeddingTag;
    TTags ScanTags;

    TUploadStatus UploadStatus;

    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvLocalKMeansResponse> Response;

    // FIXME: save PrefixRows as std::vector<std::pair<TSerializedCellVec, TSerializedCellVec>> to avoid parsing
    const ui32 PrefixColumns;
    TSerializedCellVec Prefix;
    TBufferData PrefixRows;
    bool IsFirstPrefixFeed = true;
    bool IsPrefixRowsValid = true;

    bool IsExhausted = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LOCAL_KMEANS_SCAN_ACTOR;
    }

    TLocalKMeansScanBase(ui64 tabletId, const TUserTable& table,
                         const NKikimrTxDataShard::TEvLocalKMeansRequest& request,
                         const TActorId& responseActorId,
                         TAutoPtr<TEvDataShard::TEvLocalKMeansResponse>&& response,
                         TLead&& lead)
        : TActor{&TThis::StateWork}
        , Parent{request.GetParentFrom()}
        , Child{request.GetChild()}
        , MaxRounds{request.GetNeedsRounds()}
        , InitK{request.GetK()}
        , K{request.GetK()}
        , State{EState::SAMPLE}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Rng{request.GetSeed()}
        , LevelTable{request.GetLevelName()}
        , PostingTable{request.GetPostingName()}
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
        , PrefixColumns{request.GetParentFrom() == 0 && request.GetParentTo() == 0 ? 0u : 1u}
    {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        ScanTags = MakeUploadTags(table, embedding, data, EmbeddingPos, DataPos, EmbeddingTag);
        Lead.SetTags(ScanTags);
        // upload types
        {
            Ydb::Type type;
            LevelTypes = std::make_shared<NTxProxy::TUploadTypes>(3);
            type.set_type_id(NTableIndex::ClusterIdType);
            (*LevelTypes)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, type};
            (*LevelTypes)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type};
            type.set_type_id(Ydb::Type::STRING);
            (*LevelTypes)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn, type};
        }
        PostingTypes = MakeUploadTypes(table, UploadState, embedding, data);
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoison);
            Uploader = {};
        }

        auto& record = Response->Record;
        record.SetReadRows(ReadRows);
        record.SetReadBytes(ReadBytes);
        record.SetUploadRows(UploadRows);
        record.SetUploadBytes(UploadBytes);
        if (abort != EAbort::None) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::ABORTED);
        } else if (UploadStatus.IsNone() || UploadStatus.IsSuccess()) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::DONE);
        } else {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
        }
        NYql::IssuesToMessage(UploadStatus.Issues, record.MutableIssues());

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            LOG_N("Done " << Debug() << " " << Response->Record.ShortDebugString());
        } else {
            LOG_E("Failed " << Debug() << " " << Response->Record.ShortDebugString());
        }
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
        return TStringBuilder() << "TLocalKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " Parent: " << Parent << " Child: " << Child
            << " K: " << K << " Clusters: " << Clusters.size()
            << " State: " << State << " Round: " << Round << " / " << MaxRounds
            << " LevelBuf size: " << LevelBuf.Size() << " PostingBuf size: " << PostingBuf.Size()
            << " UploadTable: " << UploadTable << " UploadBuf size: " << UploadBuf.Size() << " RetryCount: " << RetryCount;
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
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite() 
                    << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/)
    {
        LOG_D("Retry upload " << Debug());

        if (UploadInProgress()) {
            RetryUpload();
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        LOG_D("Handle TEvUploadRowsResponse " << Debug()
            << " Uploader: " << (Uploader ? Uploader.ToString() : "<null>")
            << " ev->Sender: " << ev->Sender.ToString());

        if (Uploader) {
            Y_VERIFY_S(Uploader == ev->Sender, "Mismatch"
                << " Uploader: " << Uploader.ToString()
                << " Sender: " << ev->Sender.ToString());
            Uploader = {};
        } else {
            Y_ABORT_UNLESS(Driver == nullptr);
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = ev->Get()->Issues;
        if (UploadStatus.IsSuccess()) {
            UploadRows += UploadBuf.GetRows();
            UploadBytes += UploadBuf.GetBytes();
            UploadBuf.Clear();

            TryUpload(LevelBuf, LevelTable, LevelTypes, true)
                || TryUpload(PostingBuf, PostingTable, PostingTypes, true);

            Driver->Touch(EScan::Feed);
            return;
        }

        if (RetryCount < ScanSettings.GetMaxBatchRetries() && UploadStatus.IsRetriable()) {
            LOG_N("Got retriable error, " << Debug() << " " << UploadStatus.ToString());

            ctx.Schedule(GetRetryWakeupTimeoutBackoff(RetryCount), new TEvents::TEvWakeup());
            return;
        }

        LOG_N("Got error, abort scan, " << Debug() << " " << UploadStatus.ToString());

        Driver->Touch(EScan::Final);
    }

    bool ShouldWaitUpload()
    {
        if (!HasReachedLimits(LevelBuf, ScanSettings) && !HasReachedLimits(PostingBuf, ScanSettings)) {
            return false;
        }

        if (UploadInProgress()) {
            return true;
        }
        
        TryUpload(LevelBuf, LevelTable, LevelTypes, true)
            || TryUpload(PostingBuf, PostingTable, PostingTypes, true);

        return !HasReachedLimits(LevelBuf, ScanSettings) && !HasReachedLimits(PostingBuf, ScanSettings);
    }

    void UploadImpl()
    {
        LOG_D("Uploading " << Debug());

        Y_ASSERT(!UploadBuf.IsEmpty());
        Y_ASSERT(!Uploader);
        auto actor = NTxProxy::CreateUploadRowsInternal(
            this->SelfId(), UploadTable, UploadTypes, UploadBuf.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow, true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }

    void InitUpload(std::string_view table, std::shared_ptr<NTxProxy::TUploadTypes> types)
    {
        RetryCount = 0;
        UploadTable = table;
        UploadTypes = std::move(types);
        UploadImpl();
    }

    void RetryUpload()
    {
        ++RetryCount;
        UploadImpl();
    }

    bool UploadInProgress()
    {
        return !UploadBuf.IsEmpty();
    }

    bool TryUpload(TBufferData& buffer, const TString& table, const std::shared_ptr<NTxProxy::TUploadTypes>& types, bool byLimit)
    {
        if (Y_UNLIKELY(UploadInProgress())) {
            // already uploading something
            return true;
        }

        if (!buffer.IsEmpty() && (!byLimit || HasReachedLimits(buffer, ScanSettings))) {
            buffer.FlushTo(UploadBuf);
            InitUpload(table, types);
            return true;
        }

        return false;
    }

    void FormLevelRows()
    {
        std::array<TCell, 2> pk;
        std::array<TCell, 1> data;
        for (NTable::TPos pos = 0; const auto& row : Clusters) {
            pk[0] = TCell::Make(Parent);
            pk[1] = TCell::Make(Child + pos);
            data[0] = TCell{row};
            LevelBuf.AddRow(TSerializedCellVec{pk}, TSerializedCellVec::Serialize(data));
            ++pos;
        }
    }

    ui64 GetProbability()
    {
        return Rng.GenRand64();
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

    void StartNewPrefix() {
        Round = 0;
        K = InitK;
        State = EState::SAMPLE;
        Lead.Valid = true;
        Lead.Key = TSerializedCellVec(Prefix.GetCells()); // seek to (prefix, inf)
        Lead.Relation = NTable::ESeek::Upper;
        Prefix = {};
        IsFirstPrefixFeed = true;
        IsPrefixRowsValid = true;
        PrefixRows.Clear();
        MaxProbability = std::numeric_limits<ui64>::max();
        MaxRows.clear();
        Clusters.clear();
        ClusterSizes.clear();
        AggregatedClusters.clear();
    }

public:
    TLocalKMeansScan(ui64 tabletId, const TUserTable& table, NKikimrTxDataShard::TEvLocalKMeansRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvLocalKMeansResponse>&& response,
        TLead&& lead)
        : TLocalKMeansScanBase{tabletId, table, request, responseActorId, std::move(response), std::move(lead)}
    {
        this->Dimensions = request.GetSettings().vector_dimension();
        LOG_I("Create " << Debug());
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
        LOG_D("Seek " << seq << " " << Debug());

        if (IsExhausted) {
            if (UploadInProgress()
                || TryUpload(LevelBuf, LevelTable, LevelTypes, false)
                || TryUpload(PostingBuf, PostingTable, PostingTypes, false))
            {
                return EScan::Sleep;
            }
            return EScan::Final;
        }

        lead = Lead;

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountBytes(key, row);

        if (PrefixColumns && Prefix && !TCellVectorsEquals{}(Prefix.GetCells(), key.subspan(0, PrefixColumns))) {
            if (!FinishPrefix()) {
                // scan current prefix rows with a new state again
                return EScan::Reset;
            }
        }

        if (PrefixColumns && !Prefix) {
            Prefix = TSerializedCellVec{key.subspan(0, PrefixColumns)};
            auto newParent = key.at(0).template AsValue<ui64>();
            Child += (newParent - Parent) * InitK;
            Parent = newParent;
        }

        if (IsFirstPrefixFeed && IsPrefixRowsValid) {
            PrefixRows.AddRow(TSerializedCellVec{key}, TSerializedCellVec::Serialize(*row));
            if (HasReachedLimits(PrefixRows, ScanSettings)) {
                PrefixRows.Clear();
                IsPrefixRowsValid = false;
            }
        }

        Feed(key, *row);

        return ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_D("Exhausted " << Debug());

        if (!FinishPrefix()) {
            return EScan::Reset;
        }
            
        IsExhausted = true;
        
        // call Seek to wait uploads
        return EScan::Reset;
    }

private:
    bool FinishPrefix()
    {
        if (FinishPrefixImpl()) {
            StartNewPrefix();
            LOG_D("FinishPrefix finished " << Debug());
            return true;
        } else {
            IsFirstPrefixFeed = false;
            
            if (IsPrefixRowsValid) {
                LOG_D("FinishPrefix not finished, manually feeding " << PrefixRows.Size() << " saved rows " << Debug());
                for (ui64 iteration = 0; ; iteration++) {
                    for (const auto& [key, row_] : *PrefixRows.GetRowsData()) {
                        TSerializedCellVec row(row_);
                        Feed(key.GetCells(), row.GetCells());
                    }
                    if (FinishPrefixImpl()) {
                        StartNewPrefix();
                        LOG_D("FinishPrefix finished in " << iteration << " iterations " << Debug());
                        return true;
                    } else {
                        LOG_D("FinishPrefix not finished in " << iteration << " iterations " << Debug());
                    }
                }
            } else {
                LOG_D("FinishPrefix not finished, rescanning rows " << Debug());
            }

            return false;
        }
    }

    bool FinishPrefixImpl()
    {
        if (State == EState::SAMPLE) {
            State = EState::KMEANS;
            if (!InitAggregatedClusters()) {
                // We don't need to do anything,
                // because this datashard doesn't have valid embeddings for this prefix
                return true;
            }
            Round = 1;
            return false; // do KMEANS
        }

        if (State == EState::KMEANS) {
            if (RecomputeClusters()) {
                FormLevelRows();
                State = UploadState;
                return false; // do UPLOAD_*
            } else {
                ++Round;
                return false; // recompute KMEANS
            }
        }

        if (State == UploadState) {
            return true;
        }

        Y_ASSERT(false);
        return true;
    }

    bool InitAggregatedClusters()
    {
        if (Clusters.size() == 0) {
            return false;
        }
        if (Clusters.size() < K) {
            // if this datashard have less than K valid embeddings for this parent
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

    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        switch (State) {
            case EState::SAMPLE:
                FeedSample(row);
                break;
            case EState::KMEANS:
                FeedKMeans(row);
                break;
            case EState::UPLOAD_MAIN_TO_BUILD:
                FeedUploadMain2Build(key, row);
                break;
            case EState::UPLOAD_MAIN_TO_POSTING:
                FeedUploadMain2Posting(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_BUILD:
                FeedUploadBuild2Build(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_POSTING:
                FeedUploadBuild2Posting(key, row);
                break;
            default:
                Y_ASSERT(false);
        }
    }

    void FeedSample(TArrayRef<const TCell> row)
    {
        const auto embedding = row.at(EmbeddingPos).AsRef();
        if (!this->IsExpectedSize(embedding)) {
            return;
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
    }

    void FeedKMeans(TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        AggregateToCluster(pos, row.at(EmbeddingPos).Data());
    }

    void FeedUploadMain2Build(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos < K) {
            AddRowMain2Build(PostingBuf, Child + pos, key, row);
        }
    }

    void FeedUploadMain2Posting(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos < K) {
            AddRowMain2Posting(PostingBuf, Child + pos, key, row, DataPos);
        }
    }

    void FeedUploadBuild2Build(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos < K) {
            AddRowBuild2Build(PostingBuf, Child + pos, key, row);
        }
    }

    void FeedUploadBuild2Posting(TArrayRef<const TCell> key, TArrayRef<const TCell> row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos < K) {
            AddRowBuild2Posting(PostingBuf, Child + pos, key, row, DataPos);
        }
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
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    auto response = MakeHolder<TEvDataShard::TEvLocalKMeansResponse>();
    response->Record.SetId(id);
    response->Record.SetTabletId(TabletID());
    response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
    response->Record.SetRequestSeqNoRound(seqNo.Round);

    LOG_N("Starting TLocalKMeansScan TabletId: " << TabletID() 
        << " " << request.ShortDebugString()
        << " row version " << rowVersion);

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }

    auto badRequest = [&](const TString& error) {
        response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
        auto issue = response->Record.AddIssues();
        issue->set_severity(NYql::TSeverityIds::S_ERROR);
        issue->set_message(error);
    };
    auto trySendBadRequest = [&] {
        if (response->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
            LOG_E("Rejecting TLocalKMeansScan bad request TabletId: " << TabletID()
                << " " << request.ShortDebugString()
                << " with response " << response->Record.ShortDebugString());
            ctx.Send(ev->Sender, std::move(response));
            return true;
        } else {
            return false;
        }
    };

    // 1. Validating table and path existence
    if (request.GetTabletId() != TabletID()) {
        badRequest(TStringBuilder() << "Wrong shard " << request.GetTabletId() << " this is " << TabletID());
    }
    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is " << State << " and not ready for requests");
    }
    const auto pathId = TPathId::FromProto(request.GetPathId());
    const auto* userTableIt = GetUserTables().FindPtr(pathId.LocalPathId);
    if (!userTableIt) {
        badRequest(TStringBuilder() << "Unknown table id: " << pathId.LocalPathId);
    }
    if (trySendBadRequest()) {
        return;
    }
    const auto& userTable = **userTableIt;

    // 2. Validating request fields
    if (request.HasSnapshotStep() || request.HasSnapshotTxId()) {
        const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
        if (!SnapshotManager.FindAvailable(snapshotKey)) {
            badRequest(TStringBuilder() << "Unknown snapshot for path id " << pathId.OwnerId << ":" << pathId.LocalPathId
                << ", snapshot step is " << snapshotKey.Step << ", snapshot tx is " << snapshotKey.TxId);
        }
    }

    if (request.GetUpload() != NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD
        && request.GetUpload() != NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING
        && request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
        && request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING)
    {
        badRequest("Wrong upload");
    }

    if (request.GetK() < 2) {
        badRequest("Should be requested partition on at least two rows");
    }

    const auto parentFrom = request.GetParentFrom();
    const auto parentTo = request.GetParentTo();
    NTable::TLead lead;
    if (parentFrom == 0 && parentTo == 0) {
        lead.To({}, NTable::ESeek::Lower);
    } else if (parentFrom > parentTo) {
        badRequest(TStringBuilder() << "Parent from " << parentFrom << " should be less or equal to parent to " << parentTo);
    } else {
        TCell from = TCell::Make(parentFrom - 1);
        TCell to = TCell::Make(parentTo);
        TTableRange range{{&from, 1}, false, {&to, 1}, true};
        auto scanRange = Intersect(userTable.KeyColumnTypes, range, userTable.Range.ToTableRange());
        if (scanRange.IsEmptyRange(userTable.KeyColumnTypes)) {
            badRequest(TStringBuilder() << "Requested range doesn't intersect with table range:"
                << " requestedRange: " << DebugPrintRange(userTable.KeyColumnTypes, range, *AppData()->TypeRegistry)
                << " tableRange: " << DebugPrintRange(userTable.KeyColumnTypes, userTable.Range.ToTableRange(), *AppData()->TypeRegistry)
                << " scanRange: " << DebugPrintRange(userTable.KeyColumnTypes, scanRange, *AppData()->TypeRegistry));
        }
        lead.To(range.From, NTable::ESeek::Upper);
        lead.Until(range.To, true);
    }

    if (!request.HasLevelName()) {
        badRequest(TStringBuilder() << "Empty level table name");
    }
    if (!request.HasPostingName()) {
        badRequest(TStringBuilder() << "Empty posting table name");
    }

    auto tags = GetAllTags(userTable);
    if (!tags.contains(request.GetEmbeddingColumn())) {
        badRequest(TStringBuilder() << "Unknown embedding column: " << request.GetEmbeddingColumn());
    }
    for (auto dataColumn : request.GetDataColumns()) {
        if (!tags.contains(dataColumn)) {
            badRequest(TStringBuilder() << "Unknown data column: " << dataColumn);
        }
    }

    if (trySendBadRequest()) {
        return;
    }

    // 3. Validating vector index settings
    TAutoPtr<NTable::IScan> scan;
    auto createScan = [&]<typename T> {
        scan = new TLocalKMeansScan<T>{
            TabletID(), userTable, request, ev->Sender, std::move(response),
            std::move(lead)
        };
    };
    MakeScan(request, createScan, badRequest);
    if (!scan) {
        auto sent = trySendBadRequest();
        Y_ENSURE(sent);
        return;
    }

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

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?
    const auto scanId = QueueScan(userTable.LocalTid, std::move(scan), 0, scanOpts);
    ScanManager.Set(id, seqNo).push_back(scanId);
}

}
