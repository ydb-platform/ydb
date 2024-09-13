#include "datashard_impl.h"
#include "range_ops.h"
#include "scan_common.h"
#include "upload_stats.h"
#include "buffer_data.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/common/kqp_types.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/tablet_flat/flat_row_state.h>

#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/core/ydb_convert/table_description.h>
#include <ydb/core/ydb_convert/ydb_convert.h>
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

#include <library/cpp/dot_product/dot_product.h>
#include <library/cpp/l1_distance/l1_distance.h>
#include <library/cpp/l2_distance/l2_distance.h>

#define LOG_T(stream) LOG_TRACE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_D(stream) LOG_DEBUG_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_N(stream) LOG_NOTICE_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)
#define LOG_E(stream) LOG_ERROR_S(*TlsActivationContext, NKikimrServices::TX_DATASHARD, stream)

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const float* lhs, const float* rhs, size_t length) noexcept {
    auto r = TriWayDotProduct(lhs, rhs, length);
    return {static_cast<TRes>(r.LL), static_cast<TRes>(r.LR), static_cast<TRes>(r.RR)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const i8* lhs, const i8* rhs, size_t length) noexcept {
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

template <typename TRes>
Y_PURE_FUNCTION TTriWayDotProduct<TRes> CosineImpl(const ui8* lhs, const ui8* rhs, size_t length) noexcept {
    const auto ll = DotProduct(lhs, lhs, length);
    const auto lr = DotProduct(lhs, rhs, length);
    const auto rr = DotProduct(rhs, rhs, length);
    return {static_cast<TRes>(ll), static_cast<TRes>(lr), static_cast<TRes>(rr)};
}

namespace NKikimr::NDataShard {

TTableRange CreateRangeFrom(const TUserTable& table, ui32 parent, TCell& from, TCell& to) {
    if (parent == 0) {
        return table.GetTableRange();
    }
    from = TCell::Make(parent - 1);
    to = TCell::Make(parent);
    TTableRange range{{&from, 1}, false, {&to, 1}, true};
    return Intersect(table.KeyColumnTypes, range, table.GetTableRange());
}

NTable::TLead CreateLeadFrom(const TTableRange& range) {
    NTable::TLead lead;
    if (range.From) {
        lead.To(range.From, range.InclusiveFrom ? NTable::ESeek::Lower : NTable::ESeek::Upper);
    } else {
        lead.To({}, NTable::ESeek::Lower);
    }
    if (range.To) {
        lead.Until(range.To, range.InclusiveTo);
    }
    return lead;
}

// TODO(mbkkt) separate implementation for bit
template <typename T>
struct TMetric {
    using TCoord = T;
    // TODO(mbkkt) maybe compute floating sum in double? Needs benchmark
    using TSum = std::conditional_t<std::is_floating_point_v<T>, T, int64_t>;

    ui32 Dimensions = 0;

    bool IsExpectedSize(TArrayRef<const char> data) const noexcept {
        return data.size() == 1 + sizeof(TCoord) * Dimensions;
    }

    auto GetCoords(const char* coords) {
        return std::span{reinterpret_cast<const TCoord*>(coords), Dimensions};
    }

    auto GetData(char* data) {
        return std::span{reinterpret_cast<TCoord*>(data), Dimensions};
    }

    void Fill(TString& d, TSum* embedding, ui64& c) {
        const auto count = static_cast<TSum>(std::exchange(c, 0));
        auto data = GetData(d.MutRef().data());
        for (auto& coord : data) {
            coord = *embedding / count;
            *embedding++ = 0;
        }
    }
};

template <typename T>
struct TCosineSimilarity: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    // double used to avoid precision issues
    using TRes = double;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const auto r = CosineImpl<TRes>(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        // sqrt(ll) * sqrt(rr) computed instead of sqrt(ll * rr) to avoid precision issues
        const auto norm = std::sqrt(r.LL) * std::sqrt(r.RR);
        const TRes similarity = norm != 0 ? static_cast<TRes>(r.LR) / static_cast<TRes>(norm) : 0;
        return -similarity;
    }
};

template <typename T>
struct TL1Distance: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, ui64>;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const auto distance = L1Distance(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return distance;
    }
};

template <typename T>
struct TL2Distance: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, ui64>;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const auto distance = L2SqrDistance(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return distance;
    }
};

template <typename T>
struct TMaxInnerProductSimilarity: TMetric<T> {
    using TCoord = typename TMetric<T>::TCoord;
    using TSum = typename TMetric<T>::TSum;
    using TRes = std::conditional_t<std::is_floating_point_v<T>, T, i64>;

    static TRes Init() {
        return std::numeric_limits<TRes>::max();
    }

    auto Distance(const char* cluster, const char* embedding) const noexcept {
        const TRes similarity = DotProduct(reinterpret_cast<const TCoord*>(cluster), reinterpret_cast<const TCoord*>(embedding), this->Dimensions);
        return -similarity;
    }
};

template <typename TMetric>
struct TCalculation: TMetric {
    ui32 FindClosest(std::span<const TString> clusters, const char* embedding) {
        auto min = this->Init();
        ui32 closest = std::numeric_limits<ui32>::max();
        for (size_t i = 0; const auto& cluster : clusters) {
            auto distance = this->Distance(cluster.data(), embedding);
            if (distance < min) {
                min = distance;
                closest = i;
            }
            ++i;
        }
        return closest;
    }
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

    // Sample
    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

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

    // Response
    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvLocalKMeansProgressResponse> Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::LOCAL_KMEANS_SCAN_ACTOR;
    }

    TLocalKMeansScanBase(const TUserTable& table, TLead&& lead, const NKikimrTxDataShard::TEvLocalKMeansRequest& request, const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvLocalKMeansProgressResponse>&& response)
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
        , Response{std::move(response)} {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        {
            auto tags = GetAllTags(table);
            KMeansScan = tags.at(embedding);
            UploadScan.reserve(1 + data.size());
            if (auto it = std::find(data.begin(), data.end(), embedding); it != data.end()) {
                EmbeddingPos = it - data.begin();
                DataPos = 0;
            } else {
                UploadScan.push_back(KMeansScan);
            }
            for (const auto& column : data) {
                UploadScan.push_back(tags.at(column));
            }
        }
        // upload types
        Ydb::Type type;
        if (State <= EState::KMEANS) {
            TargetTypes = std::make_shared<NTxProxy::TUploadTypes>(3);
            type.set_type_id(Ydb::Type::UINT32);
            (*TargetTypes)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::LevelTable_ParentIdColumn, type};
            (*TargetTypes)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::LevelTable_IdColumn, type};
            type.set_type_id(Ydb::Type::STRING);
            (*TargetTypes)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::LevelTable_EmbeddingColumn, type};
        }
        {
            auto types = GetAllTypes(table);

            NextTypes = std::make_shared<NTxProxy::TUploadTypes>();
            NextTypes->reserve(1 + 1 + std::min(table.KeyColumnTypes.size() + data.size(), types.size()));

            type.set_type_id(Ydb::Type::UINT32);
            NextTypes->emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn, type);

            auto addType = [&](const auto& column) {
                auto it = types.find(column);
                Y_ABORT_UNLESS(it != types.end());
                ProtoYdbTypeFromTypeInfo(&type, it->second);
                NextTypes->emplace_back(it->first, type);
                types.erase(it);
            };
            for (const auto& column : table.KeyColumnIds) {
                addType(table.Columns.at(column).Name);
            }
            switch (UploadState) {
                case EState::UPLOAD_MAIN_TO_TMP:
                case EState::UPLOAD_TMP_TO_TMP:
                    addType(embedding);
                    [[fallthrough]];
                case EState::UPLOAD_MAIN_TO_POSTING:
                case EState::UPLOAD_TMP_TO_POSTING: {
                    for (const auto& column : data) {
                        addType(column);
                    }
                } break;
                default:
                    Y_UNREACHABLE();
            }
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_T("Prepare " << Debug());

        Driver = driver;
        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final {
        LOG_T("Finish " << Debug());

        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoisonPill);
            Uploader = {};
        }

        auto& record = Response->Record;
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

    void Describe(IOutputStream& out) const noexcept final {
        out << Debug();
    }

    TString Debug() const {
        auto builder = TStringBuilder() << " TLocalKMeansScan";
        if (Response) {
            auto& r = Response->Record;
            builder << " Id: " << r.GetId();
        }
        return builder << " State: " << State
                       << " Round: " << Round
                       << " MaxRounds: " << MaxRounds
                       << " ReadBuf size: " << ReadBuf.Size()
                       << " WriteBuf size: " << WriteBuf.Size()
                       << " ";
    }

    EScan PageFault() noexcept final {
        LOG_T("PageFault " << Debug());

        if (!ReadBuf.IsEmpty() && WriteBuf.IsEmpty()) {
            ReadBuf.FlushTo(WriteBuf);
            Upload(false);
        }

        return EScan::Feed;
    }

protected:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("TLocalKMeansScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/) {
        LOG_T("Retry upload " << Debug());

        if (!WriteBuf.IsEmpty()) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx) {
        LOG_T("Handle TEvUploadRowsResponse "
              << Debug()
              << " Uploader: " << Uploader.ToString()
              << " ev->Sender: " << ev->Sender.ToString());

        if (Uploader) {
            Y_VERIFY_S(Uploader == ev->Sender, "Mismatch Uploader: " << Uploader.ToString() << " ev->Sender: " << ev->Sender.ToString() << Debug());
        } else {
            Y_ABORT_UNLESS(Driver == nullptr);
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = ev->Get()->Issues;
        if (UploadStatus.IsSuccess()) {
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

    EScan FeedUpload() {
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

    ui64 GetProbability() {
        return Rng.GenRand64();
    }

    void Upload(bool isRetry) {
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
            this->SelfId(), TargetTable,
            TargetTypes,
            WriteBuf.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow,
            true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }

    void UploadSample() {
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
        ui64 Count = 0;
    };
    std::vector<TAggregatedCluster> AggregatedClusters;

public:
    TLocalKMeansScan(const TUserTable& table, TLead&& lead, NKikimrTxDataShard::TEvLocalKMeansRequest& request, const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvLocalKMeansProgressResponse>&& response)
        : TLocalKMeansScanBase{table, std::move(lead), request, responseActorId, std::move(response)} {
        this->Dimensions = request.GetSettings().vector_dimension();
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
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
                return EScan::Final;
            }
            ++Round;
            return EScan::Feed;
        }

        Y_ASSERT(State == EState::KMEANS);
        RecomputeClusters(Round >= MaxRounds);
        if (Round >= MaxRounds) {
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

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final {
        LOG_T("Feed " << Debug());
        switch (State) {
            case EState::SAMPLE:
                return FeedSample(row);
            case EState::KMEANS:
                return FeedKMeans(row);
            case EState::UPLOAD_MAIN_TO_TMP:
                return FeedUploadMain2Tmp(key, row);
            case EState::UPLOAD_MAIN_TO_POSTING:
                return FeedUploadMain2Posting(key, row);
            case EState::UPLOAD_TMP_TO_TMP:
                return FeedUploadTmp2Tmp(key, row);
            case EState::UPLOAD_TMP_TO_POSTING:
                return FeedUploadTmp2Posting(key, row);
            default:
                return EScan::Final;
        }
    }

private:
    bool InitAggregatedClusters() {
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
        AggregatedClusters.resize(K);
        for (auto& aggregate : AggregatedClusters) {
            aggregate.Cluster.resize(this->Dimensions, 0);
        }
        return true;
    }

    void AggregateToCluster(ui32 pos, const char* embedding) {
        if (pos >= K) {
            return;
        }
        auto& aggregate = AggregatedClusters[pos];
        auto* coords = aggregate.Cluster.data();
        for (auto coord : this->GetCoords(embedding)) {
            *coords++ += coord;
        }
        ++aggregate.Count;
    }

    void RecomputeClusters(bool last) {
        auto r = Clusters.begin();
        auto w = r;
        for (auto& aggregate : AggregatedClusters) {
            if (aggregate.Count != 0) {
                auto& cluster = *r;
                this->Fill(cluster, aggregate.Cluster.data(), aggregate.Count);
                if (w != r) {
                    Y_ASSERT(w < r);
                    *w = std::move(*r);
                }
                ++w;
            } else if (!last) {
                ++w;
            }
            ++r;
        }
        Clusters.erase(w, Clusters.end());
    }

    ui32 FeedEmbedding(const TRow& row, NTable::TPos embeddingPos) {
        Y_ASSERT(embeddingPos < row.Size());
        const auto embedding = row.Get(embeddingPos).AsRef();
        ++ReadRows;
        ReadBytes += embedding.size(); // TODO(mbkkt) add some constant overhead?
        if (!this->IsExpectedSize(embedding)) {
            return std::numeric_limits<ui32>::max();
        }
        return this->FindClosest(Clusters, embedding.data());
    }

    EScan FeedSample(const TRow& row) noexcept {
        Y_ASSERT(row.Size() == 1);
        const auto embedding = row.Get(0).AsRef();
        ++ReadRows;
        ReadBytes += embedding.size(); // TODO(mbkkt) add some constant overhead?
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

    EScan FeedKMeans(const TRow& row) noexcept {
        Y_ASSERT(row.Size() == 1);
        const ui32 pos = FeedEmbedding(row, 0);
        AggregateToCluster(pos, row.Get(0).Data());
        return EScan::Feed;
    }

    EScan FeedUploadMain2Tmp(TArrayRef<const TCell> key, const TRow& row) noexcept {
        const ui32 pos = FeedEmbedding(row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        std::array<TCell, 1> cells;
        cells[0] = TCell::Make(Child + pos);
        auto pk = TSerializedCellVec::Serialize(cells);
        TSerializedCellVec::UnsafeAppendCells(key, pk);
        ReadBuf.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize(*row));
        return FeedUpload();
    }

    EScan FeedUploadMain2Posting(TArrayRef<const TCell> key, const TRow& row) noexcept {
        const ui32 pos = FeedEmbedding(row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        std::array<TCell, 1> cells;
        cells[0] = TCell::Make(Child + pos);
        auto pk = TSerializedCellVec::Serialize(cells);
        TSerializedCellVec::UnsafeAppendCells(key, pk);
        ReadBuf.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize((*row).Slice(DataPos)));
        return FeedUpload();
    }

    EScan FeedUploadTmp2Tmp(TArrayRef<const TCell> key, const TRow& row) noexcept {
        const ui32 pos = FeedEmbedding(row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        std::array<TCell, 1> cells;
        cells[0] = TCell::Make(Child + pos);
        auto pk = TSerializedCellVec::Serialize(cells);
        TSerializedCellVec::UnsafeAppendCells(key.Slice(1), pk);
        ReadBuf.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize(*row));
        return FeedUpload();
    }

    EScan FeedUploadTmp2Posting(TArrayRef<const TCell> key, const TRow& row) noexcept {
        const ui32 pos = FeedEmbedding(row, EmbeddingPos);
        if (pos > K) {
            return EScan::Feed;
        }
        std::array<TCell, 1> cells;
        cells[0] = TCell::Make(Child + pos);
        auto pk = TSerializedCellVec::Serialize(cells);
        TSerializedCellVec::UnsafeAppendCells(key.Slice(1), pk);
        ReadBuf.AddRow(TSerializedCellVec{key}, TSerializedCellVec{std::move(pk)}, TSerializedCellVec::Serialize((*row).Slice(DataPos)));
        return FeedUpload();
    }
};

class TDataShard::TTxHandleSafeLocalKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeLocalKMeansScan(TDataShard* self, TEvDataShard::TEvLocalKMeansRequest::TPtr&& ev)
        : TTransactionBase(self)
        , Ev(std::move(ev)) {
    }

    bool Execute(TTransactionContext&, const TActorContext& ctx) final {
        Self->HandleSafe(Ev, ctx);
        return true;
    }

    void Complete(const TActorContext&) final {
    }

private:
    TEvDataShard::TEvLocalKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvLocalKMeansRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeLocalKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvLocalKMeansRequest::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion,
                                                     std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }
    const ui64 id = record.GetId();

    auto response = MakeHolder<TEvDataShard::TEvLocalKMeansProgressResponse>();
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

    const auto pathId = PathIdFromPathId(record.GetPathId());
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

    if (!record.HasSnapshotStep() || !record.HasSnapshotTxId()) {
        badRequest(TStringBuilder() << " request doesn't have Shapshot Step or TxId");
        return;
    }

    const TSnapshotKey snapshotKey(pathId, rowVersion.Step, rowVersion.TxId);
    const TSnapshot* snapshot = SnapshotManager.FindAvailable(snapshotKey);
    if (!snapshot) {
        badRequest(TStringBuilder()
                   << "no snapshot has been found"
                   << " , path id is " << pathId.OwnerId << ":" << pathId.LocalPathId
                   << " , snapshot step is " << snapshotKey.Step
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

    if (!record.HasEmbeddingColumn()) {
        badRequest(TStringBuilder() << "Should be specified embedding column");
        return;
    }

    const auto& settings = record.GetSettings();
    if (settings.vector_dimension() < 1) {
        badRequest(TStringBuilder() << "Dimension of vector should be at least one");
        return;
    }
    TAutoPtr<NTable::IScan> scan;

    auto createScan = [&]<typename T> {
        scan = new TLocalKMeansScan<T>{
            userTable,
            CreateLeadFrom(range),
            record,
            ev->Sender,
            std::move(response),
        };
    };

    auto handleType = [&]<template <typename...> typename T>() {
        switch (settings.vector_type()) {
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_FLOAT:
                return createScan.operator()<T<float>>();
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_UINT8:
                return createScan.operator()<T<ui8>>();
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_INT8:
                return createScan.operator()<T<i8>>();
            case Ydb::Table::VectorIndexSettings::VECTOR_TYPE_BIT:
                return badRequest("TODO(mbkkt) bit vector type is not supported");
            default:
                return badRequest("Wrong vector type");
        }
    };

    // TODO(mbkkt) unify distance and similarity to single field in proto
    if (settings.has_similarity() && settings.has_distance()) {
        badRequest("Shouldn't be specified similarity and distance at the same time");
    } else if (settings.has_similarity()) {
        switch (settings.similarity()) {
            case Ydb::Table::VectorIndexSettings::SIMILARITY_COSINE:
                handleType.template operator()<TCosineSimilarity>();
                break;
            case Ydb::Table::VectorIndexSettings::SIMILARITY_INNER_PRODUCT:
                handleType.template operator()<TMaxInnerProductSimilarity>();
                break;
            default:
                badRequest("Wrong similarity");
                break;
        }
    } else if (settings.has_distance()) {
        switch (settings.distance()) {
            case Ydb::Table::VectorIndexSettings::DISTANCE_COSINE:
                // We don't need to have separate implementation for distance, because clusters will be same as for similarity
                handleType.template operator()<TCosineSimilarity>();
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_MANHATTAN:
                handleType.template operator()<TL1Distance>();
                break;
            case Ydb::Table::VectorIndexSettings::DISTANCE_EUCLIDEAN:
                handleType.template operator()<TL2Distance>();
                break;
            default:
                badRequest("Wrong distance");
                break;
        }
    } else {
        badRequest("Should be specified similarity or distance");
    }
    if (!scan) {
        return;
    }

    TScanOptions scanOpts;
    scanOpts.SetSnapshotRowVersion(rowVersion);
    scanOpts.SetResourceBroker("build_index", 10); // TODO(mbkkt) Should be different group?
    const auto scanId = QueueScan(userTable.LocalTid, std::move(scan), ev->Cookie, scanOpts);
    TScanRecord recCard = {scanId, seqNo};
    ScanManager.Set(id, recCard);
}

}
