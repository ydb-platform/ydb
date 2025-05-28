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

// This scan needed to run kmeans reshuffle which is part of global kmeans run.
static constexpr double MinVectorsNeedsReassigned = 0.01;

class TPrefixKMeansScanBase: public TActor<TPrefixKMeansScanBase>, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::TEvLocalKMeansRequest;

    NTableIndex::TClusterId Parent = 0;
    NTableIndex::TClusterId Child = 0;

    ui32 Round = 0;
    const ui32 MaxRounds = 0;

    const ui32 InitK = 0;
    ui32 K = 0;

    EState::EState State;
    const EState::EState UploadState;

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

        auto operator<=>(const TProbability&) const noexcept = default;
    };

    std::vector<TProbability> MaxRows;
    std::vector<TString> Clusters;
    std::vector<ui64> ClusterSizes;

    // Upload
    std::shared_ptr<NTxProxy::TUploadTypes> LevelTypes;
    std::shared_ptr<NTxProxy::TUploadTypes> PostingTypes;
    std::shared_ptr<NTxProxy::TUploadTypes> PrefixTypes;
    std::shared_ptr<NTxProxy::TUploadTypes> UploadTypes;

    const TString LevelTable;
    const TString PostingTable;
    const TString PrefixTable;
    TString UploadTable;

    TBufferData PostingBuf;
    TBufferData PrefixBuf;
    TBufferData UploadBuf;

    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    ui32 RetryCount = 0;

    TActorId Uploader;
    TUploadLimits Limits;

    NTable::TTag EmbeddingTag;
    TTags UploadScan;

    TUploadStatus UploadStatus;

    ui64 UploadRows = 0;
    ui64 UploadBytes = 0;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvPrefixKMeansResponse> Response;

    ui32 PrefixColumns;
    TSerializedCellVec Prefix;
    bool HasNextPrefix = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LOCAL_KMEANS_SCAN_ACTOR;
    }

    TPrefixKMeansScanBase(const TUserTable& table, TLead&& lead,
                          const NKikimrTxDataShard::TEvPrefixKMeansRequest& request,
                          const TActorId& responseActorId,
                          TAutoPtr<TEvDataShard::TEvPrefixKMeansResponse>&& response)
        : TActor{&TThis::StateWork}
        , Parent{request.GetChild()}
        , Child{Parent + 1}
        , MaxRounds{request.GetNeedsRounds()}
        , InitK{request.GetK()}
        , K{request.GetK()}
        , State{EState::SAMPLE}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , BuildId{request.GetId()}
        , Rng{request.GetSeed()}
        , LevelTable{request.GetLevelName()}
        , PostingTable{request.GetPostingName()}
        , PrefixTable{request.GetPrefixName()}
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
        , PrefixColumns{request.GetPrefixColumns()}
    {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        UploadScan = MakeUploadTags(table, embedding, data, EmbeddingPos, DataPos, EmbeddingTag);
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
        PostingTypes = MakeUploadTypes(table, UploadState, embedding, data, PrefixColumns);
        {
            auto types = GetAllTypes(table);

            PrefixTypes = std::make_shared<NTxProxy::TUploadTypes>();
            PrefixTypes->reserve(1 + PrefixColumns);

            Ydb::Type type;
            type.set_type_id(NTableIndex::ClusterIdType);
            PrefixTypes->emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type);

            auto addType = [&](const auto& column) {
                auto it = types.find(column);
                if (it != types.end()) {
                    NScheme::ProtoFromTypeInfo(it->second, type);
                    PrefixTypes->emplace_back(it->first, type);
                    types.erase(it);
                }
            };
            for (const auto& column : table.KeyColumnIds | std::views::take(PrefixColumns)) {
                addType(table.Columns.at(column).Name);
            }
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_D("Prepare " << Debug());

        Driver = driver;
        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        LOG_D("Finish " << Debug());

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
        return TStringBuilder() << "TPrefixKMeansScan Id: " << BuildId << " Parent: " << Parent << " Child: " << Child
            << " UploadTable: " << UploadTable << " K: " << K << " Clusters: " << Clusters.size()
            << " State: " << State << " Round: " << Round << " / " << MaxRounds
            << " PostingBuf size: " << PostingBuf.Size() << " PrefixBuf size: " << PrefixBuf.Size() << " UploadBuf size: " << UploadBuf.Size();
    }

    EScan PageFault() noexcept final
    {
        LOG_T("PageFault " << Debug());

        if (!UploadBuf.IsEmpty()) {
            return EScan::Feed;
        }

        if (!PostingBuf.IsEmpty()) {
            PostingBuf.FlushTo(UploadBuf);
            InitUpload(PostingTable, PostingTypes);
        } else if (!PrefixBuf.IsEmpty()) {
            PrefixBuf.FlushTo(UploadBuf);
            InitUpload(PrefixTable, PrefixTypes);
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
                LOG_E("TPrefixKMeansScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: "
                                                                            << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/)
    {
        LOG_T("Retry upload " << Debug());

        if (!UploadBuf.IsEmpty()) {
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
            if (PostingBuf.IsReachLimits(Limits)) {
                PostingBuf.FlushTo(UploadBuf);
                InitUpload(PostingTable, PostingTypes);
            } else if (PrefixBuf.IsReachLimits(Limits)) {
                PrefixBuf.FlushTo(UploadBuf);
                InitUpload(PrefixTable, PrefixTypes);
            }

            Driver->Touch(EScan::Feed);
            return;
        }

        if (RetryCount < Limits.MaxUploadRowsRetryCount && UploadStatus.IsRetriable()) {
            LOG_N("Got retriable error, " << Debug() << " " << UploadStatus.ToString());

            ctx.Schedule(Limits.GetTimeoutBackoff(RetryCount), new TEvents::TEvWakeup());
            return;
        }

        LOG_N("Got error, abort scan, " << Debug() << " " << UploadStatus.ToString());

        Driver->Touch(EScan::Final);
    }

    EScan FeedUpload()
    {
        if (!PostingBuf.IsReachLimits(Limits) && !PrefixBuf.IsReachLimits(Limits)) {
            return EScan::Feed;
        }
        if (!UploadBuf.IsEmpty()) {
            return EScan::Sleep;
        }
        if (PostingBuf.IsReachLimits(Limits)) {
            PostingBuf.FlushTo(UploadBuf);
            InitUpload(PostingTable, PostingTypes);
        } else {
            Y_ASSERT(PrefixBuf.IsReachLimits(Limits));
            PrefixBuf.FlushTo(UploadBuf);
            InitUpload(PrefixTable, PrefixTypes);
        }
        return EScan::Feed;
    }

    ui64 GetProbability()
    {
        return Rng.GenRand64();
    }

    void UploadImpl()
    {
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


    void UploadSample()
    {
        Y_ASSERT(UploadBuf.IsEmpty());
        std::array<TCell, 2> pk;
        std::array<TCell, 1> data;
        for (NTable::TPos pos = 0; const auto& row : Clusters) {
            pk[0] = TCell::Make(Parent);
            pk[1] = TCell::Make(Child + pos);
            data[0] = TCell{row};
            UploadBuf.AddRow(TSerializedCellVec{pk}, TSerializedCellVec::Serialize(data));
            ++pos;
        }
        InitUpload(LevelTable, LevelTypes);
    }
};

template <typename TMetric>
class TPrefixKMeansScan final: public TPrefixKMeansScanBase, private TCalculation<TMetric> {
    // KMeans
    using TEmbedding = std::vector<typename TMetric::TSum>;

    struct TAggregatedCluster {
        TEmbedding Cluster;
        ui64 Size = 0;
    };
    std::vector<TAggregatedCluster> AggregatedClusters;


    bool MoveToNextPrefix() {
        if (!HasNextPrefix) {
            if (UploadStatus.IsNone()) {
                UploadStatus.StatusCode = Ydb::StatusIds::SUCCESS;
            }
            return false;
        }
        Parent = Child + K;
        Child = Parent + 1;
        Round = 0;
        K = InitK;
        State = EState::SAMPLE;
        // TODO(mbkkt) Upper or Lower doesn't matter here, because we seek to (prefix, inf)
        // so we can choose Lower if it's faster.
        // Exact seek with Lower also possible but needs to rewrite some code in Feed
        Lead.To(Prefix.GetCells(), NTable::ESeek::Upper);
        Prefix = {};
        MaxProbability = std::numeric_limits<ui64>::max();
        MaxRows.clear();
        Clusters.clear();
        ClusterSizes.clear();
        HasNextPrefix = false;
        AggregatedClusters.clear();
        return true;
    }

public:
    TPrefixKMeansScan(const TUserTable& table, TLead&& lead, NKikimrTxDataShard::TEvPrefixKMeansRequest& request,
                      const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvPrefixKMeansResponse>&& response)
        : TPrefixKMeansScanBase{table, std::move(lead), request, responseActorId, std::move(response)}
    {
        this->Dimensions = request.GetSettings().vector_dimension();
        LOG_D("Create " << Debug());
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
        LOG_D("Seek " << Debug());
        ui64 zeroSeq = 0;
        while (true) {
            if (State == UploadState) {
                // TODO: it's a little suboptimal to wait here
                // better is wait after MoveToNextKey but before UploadSample
                if (!UploadBuf.IsEmpty()) {
                    return EScan::Sleep;
                }
                if (MoveToNextPrefix()) {
                    zeroSeq = seq;
                    continue;
                }
                if (!PostingBuf.IsEmpty()) {
                    PostingBuf.FlushTo(UploadBuf);
                    InitUpload(PostingTable, PostingTypes);
                    return EScan::Sleep;
                }
                if (!PrefixBuf.IsEmpty()) {
                    PrefixBuf.FlushTo(UploadBuf);
                    InitUpload(PrefixTable, PrefixTypes);
                    return EScan::Sleep;
                }
                return EScan::Final;
            }

            lead = Lead;
            if (State == EState::SAMPLE) {
                lead.SetTags({&EmbeddingTag, 1});
                if (seq == zeroSeq && !HasNextPrefix) {
                    return EScan::Feed;
                }
                State = EState::KMEANS;
                if (!InitAggregatedClusters()) {
                    // We don't need to do anything,
                    // because this datashard doesn't have valid embeddings for this parent
                    if (MoveToNextPrefix()) {
                        zeroSeq = seq;
                        continue;
                    }
                    return EScan::Final;
                }
                ++Round;
                return EScan::Feed;
            }

            Y_ASSERT(State == EState::KMEANS);
            if (RecomputeClusters()) {
                lead.SetTags(UploadScan);

                UploadSample();
                State = UploadState;
            } else {
                lead.SetTags({&EmbeddingTag, 1});
                ++Round;
            }
            return EScan::Feed;
        }
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final
    {
        LOG_T("Feed " << Debug());
        if (!Prefix) {
            Prefix = TSerializedCellVec{key.subspan(0, PrefixColumns)};

            // write {Prefix..., Parent} row to PrefixBuf:
            auto pk = TSerializedCellVec::Serialize(Prefix.GetCells());
            std::array<TCell, 1> cells;
            cells[0] = TCell::Make(Parent);
            TSerializedCellVec::UnsafeAppendCells(cells, pk);
            PrefixBuf.AddRow(TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize({}));
        } else if (!TCellVectorsEquals{}(Prefix.GetCells(), key.subspan(0, PrefixColumns))) {
            HasNextPrefix = true;
            return EScan::Reset;
        }
        ++ReadRows;
        ReadBytes += CountBytes(key, row);
        switch (State) {
            case EState::SAMPLE:
                return FeedSample(row);
            case EState::KMEANS:
                return FeedKMeans(row);
            case EState::UPLOAD_BUILD_TO_BUILD:
                return FeedUploadBuild2Build(key, row);
            case EState::UPLOAD_BUILD_TO_POSTING:
                return FeedUploadBuild2Posting(key, row);
            default:
                Y_ASSERT(false);
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

    EScan FeedUploadBuild2Build(TArrayRef<const TCell> key, const TRow& row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowBuild2Build(PostingBuf, Child + pos, key, row, PrefixColumns);
        return FeedUpload();
    }

    EScan FeedUploadBuild2Posting(TArrayRef<const TCell> key, const TRow& row) noexcept
    {
        const ui32 pos = FeedEmbedding(*this, Clusters, row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        AddRowBuild2Posting(PostingBuf, Child + pos, key, row, DataPos, PrefixColumns);
        return FeedUpload();
    }
};

class TDataShard::TTxHandleSafePrefixKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafePrefixKMeansScan(TDataShard* self, TEvDataShard::TEvPrefixKMeansRequest::TPtr&& ev)
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
    TEvDataShard::TEvPrefixKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvPrefixKMeansRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafePrefixKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvPrefixKMeansRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& record = ev->Get()->Record;
    TRowVersion rowVersion = GetMvccTxVersion(EMvccTxMode::ReadOnly);

    LOG_N("Starting TPrefixKMeansScan " << record.ShortDebugString()
        << " row version " << rowVersion);

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion, std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }
    const ui64 id = record.GetId();

    auto response = MakeHolder<TEvDataShard::TEvPrefixKMeansResponse>();
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
        response.Reset();
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

        for (auto scanId : recCard->ScanIds) {
            CancelScan(userTable.LocalTid, scanId);
        }
        ScanManager.Drop(id);
    }

    const auto range = userTable.GetTableRange();
    if (range.IsEmptyRange(userTable.KeyColumnTypes)) {
        badRequest(TStringBuilder() << " requested range doesn't intersect with table range");
        return;
    }

    if (!IsStateActive()) {
        badRequest(TStringBuilder() << "Shard " << TabletID() << " is not ready for requests");
        return;
    }

    if (record.GetK() < 2) {
        badRequest("Should be requested partition on at least two rows");
        return;
    }

    TAutoPtr<NTable::IScan> scan;
    auto createScan = [&]<typename T> {
        scan = new TPrefixKMeansScan<T>{
            userTable, CreateLeadFrom(range), record, ev->Sender, std::move(response),
        };
    };
    MakeScan(record, createScan, badRequest);
    if (!scan) {
        Y_ASSERT(!response);
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?
    const auto scanId = QueueScan(userTable.LocalTid, std::move(scan), 0, scanOpts);
    ScanManager.Set(id, seqNo).push_back(scanId);
}

}
