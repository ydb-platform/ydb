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
#include <ydb/library/yql/public/issue/yql_issue_message.h>

#include <util/generic/algorithm.h>
#include <util/string/builder.h>

namespace NKikimr::NDataShard {
using namespace NKMeans;

// This scan needed to run kmeans reshuffle which is part of global kmeans run.
class TReshuffleKMeansScanBase: public TActor<TReshuffleKMeansScanBase>, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::TEvLocalKMeansRequest;

    ui32 Parent = 0;
    ui32 Child = 0;

    ui32 K = 0;

    EState::EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    std::vector<TString> Clusters;

    // Upload
    std::shared_ptr<NTxProxy::TUploadTypes> TargetTypes;

    TString TargetTable;

    TBufferData ReadBuf;
    TBufferData WriteBuf;

    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    ui32 RetryCount = 0;

    TActorId Uploader;
    TUploadLimits Limits;

    TTags UploadScan;

    TUploadStatus UploadStatus;

    // Response
    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse> Response;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::RESHUFFLE_KMEANS_SCAN_ACTOR;
    }

    TReshuffleKMeansScanBase(const TUserTable& table, TLead&& lead, const NKikimrTxDataShard::TEvReshuffleKMeansRequest& request, const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse>&& response)
        : TActor{&TThis::StateWork}
        , Parent{request.GetParent()}
        , Child{request.GetChild()}
        , K{static_cast<ui32>(request.ClustersSize())}
        , UploadState{request.GetUpload()}
        , Lead{std::move(lead)}
        , Clusters{request.GetClusters().begin(), request.GetClusters().end()}
        , TargetTable{request.GetPostingName()}
        , ResponseActorId{responseActorId}
        , Response{std::move(response)} {
        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        // scan tags
        {
            auto tags = GetAllTags(table);
            UploadScan.reserve(1 + data.size());
            if (auto it = std::find(data.begin(), data.end(), embedding); it != data.end()) {
                EmbeddingPos = it - data.begin();
                DataPos = 0;
            } else {
                UploadScan.push_back(tags.at(embedding));
            }
            for (const auto& column : data) {
                UploadScan.push_back(tags.at(column));
            }
        }
        // upload types
        Ydb::Type type;
        {
            auto types = GetAllTypes(table);

            TargetTypes = std::make_shared<NTxProxy::TUploadTypes>();
            TargetTypes->reserve(1 + 1 + std::min(table.KeyColumnTypes.size() + data.size(), types.size()));

            type.set_type_id(Ydb::Type::UINT32);
            TargetTypes->emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::PostingTable_ParentIdColumn, type);

            auto addType = [&](const auto& column) {
                auto it = types.find(column);
                Y_ABORT_UNLESS(it != types.end());
                ProtoYdbTypeFromTypeInfo(&type, it->second);
                TargetTypes->emplace_back(it->first, type);
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

    EScan Seek(TLead& lead, ui64 seq) noexcept final {
        LOG_T("Seek " << Debug());
        if (seq == 0) {
            lead = std::move(Lead);
            lead.SetTags(UploadScan);
            return EScan::Feed;
        }
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

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final {
        LOG_T("Finish " << Debug());

        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoison);
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
        auto builder = TStringBuilder() << " TReshuffleKMeansScan";
        if (Response) {
            auto& r = Response->Record;
            builder << " Id: " << r.GetId();
        }
        return builder << " Upload: " << UploadState
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
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("TReshuffleKMeansScan: StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString() << " " << Debug());
        }
    }

    void HandleWakeup() {
        LOG_T("Retry upload " << Debug());

        if (!WriteBuf.IsEmpty()) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
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

            Schedule(Limits.GetTimeoutBackouff(RetryCount), new TEvents::TEvWakeup);
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

    void Upload(bool isRetry) {
        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
        }

        auto actor = NTxProxy::CreateUploadRowsInternal(
            this->SelfId(), TargetTable,
            TargetTypes,
            WriteBuf.GetRowsData(),
            NTxProxy::EUploadRowsMode::WriteToTableShadow,
            true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }
};

template <typename TMetric>
class TReshuffleKMeansScan final: public TReshuffleKMeansScanBase, private TCalculation<TMetric> {
public:
    TReshuffleKMeansScan(const TUserTable& table, TLead&& lead, NKikimrTxDataShard::TEvReshuffleKMeansRequest& request, const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvReshuffleKMeansResponse>&& response)
        : TReshuffleKMeansScanBase{table, std::move(lead), request, responseActorId, std::move(response)} {
        this->Dimensions = request.GetSettings().vector_dimension();
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final {
        LOG_T("Feed " << Debug());
        switch (UploadState) {
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

class TDataShard::TTxHandleSafeReshuffleKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeReshuffleKMeansScan(TDataShard* self, TEvDataShard::TEvReshuffleKMeansRequest::TPtr&& ev)
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
    TEvDataShard::TEvReshuffleKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvReshuffleKMeansRequest::TPtr& ev, const TActorContext&) {
    Execute(new TTxHandleSafeReshuffleKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvReshuffleKMeansRequest::TPtr& ev, const TActorContext& ctx) {
    auto& record = ev->Get()->Record;
    TRowVersion rowVersion(record.GetSnapshotStep(), record.GetSnapshotTxId());

    // Note: it's very unlikely that we have volatile txs before this snapshot
    if (VolatileTxManager.HasVolatileTxsAtSnapshot(rowVersion)) {
        VolatileTxManager.AttachWaitingSnapshotEvent(rowVersion,
                                                     std::unique_ptr<IEventHandle>(ev.Release()));
        return;
    }
    const ui64 id = record.GetId();

    auto response = MakeHolder<TEvDataShard::TEvReshuffleKMeansResponse>();
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

    if (record.ClustersSize() < 1) {
        badRequest(TStringBuilder() << "Should be requested at least single cluster");
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
        scan = new TReshuffleKMeansScan<T>{
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
