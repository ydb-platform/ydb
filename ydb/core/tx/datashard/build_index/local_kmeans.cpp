#include "kmeans_helper.h"
#include "../datashard_impl.h"
#include "../scan_common.h"
#include "../upload_stats.h"
#include "../buffer_data.h"

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

/*
 * TEvLocalKMeans executes a "local" (single-shard) K-means job across a single table shard.
 * It scans either a MAIN or BUILD table shard, while the output rows go to the BUILD or POSTING table.
 *
 * Request:
 * - The client sends TEvLocalKMeans with:
 *   - ParentFrom and ParentTo, specifying the key range in the input table shard
 *     - If ParentFrom=0 and ParentTo=0, the entire table shard is scanned
 *   - Child, serving as the base ID for the new cluster IDs computed in this local stage
 *   - The embedding column name and additional data columns to be used for K-means
 *   - K (number of clusters), initial random seed, and the vector dimensionality
 *   - Upload mode (MAIN_TO_BUILD, MAIN_TO_POSTING, BUILD_TO_BUILD or BUILD_TO_POSTING)
 *     determining input and output layouts
 *   - Names of target tables for centroids ("level") and row results ("posting" or "build")
 *
 * Execution Flow:
 * - A TLocalKMeansScan iterates over the table shard in key order, processing one rows group
 *   of each input cluster (one cluster has same parent for BUILD and the whole shard for MAIN):
 *   - SAMPLE: Samples embeddings to initialize cluster centers for this group
 *   - KMEANS: Performs iterative refinement of cluster centroids
 *   - UPLOAD: When final centroids are computed
 *     - Level: centroid vectors with assigned cluster IDs
 *     - Output: rows annotated with cluster IDs and optional data columns
 */

class TLocalKMeansScan: public TActor<TLocalKMeansScan>, public IActorExceptionHandler, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::EKMeansState;

    NTableIndex::TClusterId Parent = 0;
    NTableIndex::TClusterId Child = 0;

    EState State;
    const EState UploadState;

    NKMeans::TSampler Sampler;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TBatchRowsUploader Uploader;

    TBufferData* LevelBuf = nullptr;
    TBufferData* OutputBuf = nullptr;
    TBufferData* UploadBuf = nullptr;

    const ui32 Dimensions = 0;
    const ui32 K = 0;
    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

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

    std::unique_ptr<IClusters> Clusters;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::LOCAL_KMEANS_SCAN_ACTOR;
    }

    TLocalKMeansScan(ui64 tabletId, const TUserTable& table, const NKikimrTxDataShard::TEvLocalKMeansRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvLocalKMeansResponse>&& response,
        TLead&& lead, std::unique_ptr<IClusters>&& clusters)
        : TActor{&TThis::StateWork}
        , Parent{request.GetParentFrom()}
        , Child{request.GetChild()}
        , State{EState::SAMPLE}
        , UploadState{request.GetUpload()}
        , Sampler(request.GetK(), request.GetSeed())
        , Lead{std::move(lead)}
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Uploader(request.GetScanSettings())
        , Dimensions(request.GetSettings().vector_dimension())
        , K(request.GetK())
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
        , PrefixColumns{request.GetParentFrom() == 0 && request.GetParentTo() == 0 ? 0u : 1u}
        , Clusters(std::move(clusters))
    {
        LOG_I("Create " << Debug());

        const auto& embedding = request.GetEmbeddingColumn();
        const auto& data = request.GetDataColumns();
        ScanTags = MakeScanTags(table, embedding, data, EmbeddingPos, DataPos, EmbeddingTag);
        Lead.SetTags(ScanTags);
        {
            Ydb::Type type;
            auto levelTypes = std::make_shared<NTxProxy::TUploadTypes>(3);
            type.set_type_id(NTableIndex::ClusterIdType);
            (*levelTypes)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::ParentColumn, type};
            (*levelTypes)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type};
            type.set_type_id(Ydb::Type::STRING);
            (*levelTypes)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::CentroidColumn, type};
            LevelBuf = Uploader.AddDestination(request.GetLevelName(), std::move(levelTypes));
        }
        OutputBuf = Uploader.AddDestination(request.GetOutputName(), MakeOutputTypes(table, UploadState, embedding, data));
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(const std::exception& exc) final
    {
        Uploader.AddIssue(exc);
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());

        Uploader.Finish(record, status);

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

    bool OnUnhandledException(const std::exception& exc) final
    {
        if (!Driver) {
            return false;
        }
        Driver->Throw(exc);
        return true;
    }

    void Describe(IOutputStream& out) const final
    {
        out << Debug();
    }

    EScan PageFault() final
    {
        LOG_T("PageFault " << Debug());
        return EScan::Feed;
    }

    EScan Seek(TLead& lead, ui64 seq) final
    {
        LOG_T("Seek " << seq << " " << Debug());

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead = Lead;

        return EScan::Feed;
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) final
    {
        // LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        if (PrefixColumns && Prefix && !TCellVectorsEquals{}(Prefix.GetCells(), key.subspan(0, PrefixColumns))) {
            if (!FinishPrefix()) {
                // scan current prefix rows with a new state again
                return EScan::Reset;
            }
        }

        if (PrefixColumns && !Prefix) {
            Prefix = TSerializedCellVec{key.subspan(0, PrefixColumns)};
            auto newParent = key.at(0).template AsValue<ui64>();
            Child += (newParent - Parent) * K;
            Parent = newParent;
        }

        if (IsFirstPrefixFeed && IsPrefixRowsValid) {
            PrefixRows.AddRow(key, *row);
            if (PrefixRows.HasReachedLimits(ScanSettings)) {
                PrefixRows.Clear();
                IsPrefixRowsValid = false;
            }
        }

        Feed(key, *row);

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_T("Exhausted " << Debug());

        if (!FinishPrefix()) {
            return EScan::Reset;
        }

        IsExhausted = true;

        // call Seek to wait uploads
        return EScan::Reset;
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

        Uploader.RetryUpload();
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        LOG_D("Handle TEvUploadRowsResponse " << Debug()
            << " ev->Sender: " << ev->Sender.ToString());

        if (!Driver) {
            return;
        }

        Uploader.Handle(ev);

        if (Uploader.GetUploadStatus().IsSuccess()) {
            Driver->Touch(EScan::Feed);
            return;
        }

        if (auto retryAfter = Uploader.GetRetryAfter(); retryAfter) {
            LOG_N("Got retriable error, " << Debug() << " " << Uploader.GetUploadStatus().ToString());
            ctx.Schedule(*retryAfter, new TEvents::TEvWakeup());
            return;
        }

        LOG_N("Got error, abort scan, " << Debug() << " " << Uploader.GetUploadStatus().ToString());

        Driver->Touch(EScan::Final);
    }

    void StartNewPrefix()
    {
        State = EState::SAMPLE;
        Lead.Valid = true;
        Lead.Key = TSerializedCellVec(Prefix.GetCells()); // seek to (prefix, inf)
        Lead.Relation = NTable::ESeek::Upper;
        Prefix = {};
        IsFirstPrefixFeed = true;
        IsPrefixRowsValid = true;
        PrefixRows.Clear();
        Sampler.Finish();
        Clusters->Clear();
    }

    bool FinishPrefix()
    {
        if (FinishPrefixImpl()) {
            StartNewPrefix();
            LOG_T("FinishPrefix finished " << Debug());
            return true;
        } else {
            IsFirstPrefixFeed = false;

            if (IsPrefixRowsValid) {
                LOG_T("FinishPrefix not finished, manually feeding " << PrefixRows.GetRows() << " saved rows " << Debug());
                for (ui64 iteration = 0; ; iteration++) {
                    for (const auto& [key, row_] : *PrefixRows.GetRowsData()) {
                        TSerializedCellVec row(row_);
                        Feed(key.GetCells(), row.GetCells());
                    }
                    if (FinishPrefixImpl()) {
                        StartNewPrefix();
                        LOG_T("FinishPrefix finished in " << iteration << " iterations " << Debug());
                        return true;
                    } else {
                        LOG_T("FinishPrefix not finished in " << iteration << " iterations " << Debug());
                    }
                }
            } else {
                LOG_T("FinishPrefix not finished, rescanning rows " << Debug());
            }

            return false;
        }
    }

    bool FinishPrefixImpl()
    {
        if (State == EState::SAMPLE) {
            State = EState::KMEANS;
            auto rows = Sampler.Finish().second;
            if (rows.size() == 0) {
                // We don't need to do anything,
                // because this datashard doesn't have valid embeddings for this prefix
                return true;
            }
            if (rows.size() < K) {
                // if this datashard have less than K valid embeddings for this parent
                // lets make single centroid for it
                rows.resize(1);
            }
            bool ok = Clusters->SetClusters(std::move(rows));
            Y_ENSURE(ok);
            Clusters->SetRound(1);
            return false; // do KMEANS
        }

        if (State == EState::KMEANS) {
            if (Clusters->NextRound()) {
                FormLevelRows();
                State = UploadState;
                return false; // do UPLOAD_*
            } else {
                return false; // recompute KMEANS
            }
        }

        if (State == UploadState) {
            return true;
        }

        Y_ENSURE(false);
        return true;
    }

    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        switch (State) {
            case EState::SAMPLE:
                FeedSample(row);
                break;
            case EState::KMEANS:
                FeedKMeans(row);
                break;
            case EState::UPLOAD_MAIN_TO_BUILD:
                FeedMainToBuild(key, row);
                break;
            case EState::UPLOAD_MAIN_TO_POSTING:
                FeedMainToPosting(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_BUILD:
                FeedBuildToBuild(key, row);
                break;
            case EState::UPLOAD_BUILD_TO_POSTING:
                FeedBuildToPosting(key, row);
                break;
            default:
                Y_ENSURE(false);
        }
    }

    void FeedSample(TArrayRef<const TCell> row)
    {
        const auto embedding = row.at(EmbeddingPos).AsRef();
        if (!Clusters->IsExpectedSize(embedding)) {
            return;
        }

        Sampler.Add([&embedding](){
            return TString(embedding.data(), embedding.size());
        });
    }

    void FeedKMeans(TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            Clusters->AggregateToCluster(*pos, row.at(EmbeddingPos).AsRef());
        }
    }

    void FeedMainToBuild(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key, row, key, false);
        }
    }

    void FeedMainToPosting(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key, row.Slice(DataPos), key, true);
        }
    }

    void FeedBuildToBuild(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key.Slice(1), row, key, false);
        }
    }

    void FeedBuildToPosting(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, key.Slice(1), row.Slice(DataPos), key, true);
        }
    }

    void FormLevelRows()
    {
        const bool isPostingLevel = UploadState == NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING
            || UploadState == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING;

        for (NTable::TPos pos = 0; const auto& row : Clusters->GetClusters()) {
            AddRowToLevel(*LevelBuf, Parent, Child + pos, row, isPostingLevel);
            ++pos;
        }
    }

    TString Debug() const
    {
        return TStringBuilder() << "TLocalKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " State: " << State
            << " Parent: " << Parent << " Child: " << Child
            << " " << Sampler.Debug()
            << " " << Clusters->Debug()
            << " " << Uploader.Debug();
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

    try {
        auto response = MakeHolder<TEvDataShard::TEvLocalKMeansResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

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
        if (parentFrom == 0) {
            if (request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
                || request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING)
            {
                badRequest("Wrong upload for zero parent");
            }
            lead.To({}, NTable::ESeek::Lower);
        } else if (parentFrom > parentTo) {
            badRequest(TStringBuilder() << "Parent from " << parentFrom << " should be less or equal to parent to " << parentTo);
        } else if (request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_BUILD
            || request.GetUpload() == NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING)
        {
            badRequest("Wrong upload for non-zero parent");
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

        if (!request.GetLevelName()) {
            badRequest(TStringBuilder() << "Empty level table name");
        }
        if (!request.GetOutputName()) {
            badRequest(TStringBuilder() << "Empty output table name");
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
        TString error;
        auto clusters = NKikimr::NKMeans::CreateClusters(request.GetSettings(), request.GetNeedsRounds(), error);
        if (!clusters) {
            badRequest(error);
            auto sent = trySendBadRequest();
            Y_ENSURE(sent);
            return;
        }
        TAutoPtr<NTable::IScan> scan = new TLocalKMeansScan(
            TabletID(), userTable, request, ev->Sender, std::move(response),
            std::move(lead), std::move(clusters)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvLocalKMeansResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TLocalKMeansScan");
    }
}

}
