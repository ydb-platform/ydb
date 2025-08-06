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
 * TPrefixKMeansScan executes a "prefix-aware" K-means clustering job across a single table shard.
 * It performs clustering separately for each distinct prefix (based on specified prefix length).
 * It scans a BUILD table shard, while the output rows go to the BUILD or POSTING table.
 *
 * Request:
 * - The client sends TEvPrefixKMeansRequest with:
 *   - Child: base ID from which new cluster IDs are assigned within this request.
 *     - Each prefix group processed will be assigned cluster IDs starting at Child + 1.
 *     - For a request with K clusters per prefix, the IDs used for the first prefix group are
 *       (Child + 1) to (Child + K), and the parent ID for these is Child.
 *     - The next prefix group will increment the parent/child cluster ID range accordingly,
 *       allowing consistent hierarchical labeling.
 *   - The embedding column name and optional data columns used during clustering
 *   - K (number of clusters per prefix group), random seed, and vector dimensionality
 *   - Upload mode (BUILD_TO_BUILD or BUILD_TO_POSTING), determining output layout
 *   - Names of target tables for centroids ("level"), row results ("posting" or "build"),
 *     and the prefix mapping ("prefix")
 *
 * Execution Flow:
 * - TPrefixKMeansScan iterates over the table shard in key order, processing one prefix group at a time:
 *   - SAMPLE: Samples embeddings to initialize cluster centers for this prefix group
 *   - KMEANS: Performs iterative refinement of cluster centroids
 *   - UPLOAD: Writes results to designated tables:
 *     - Level: centroid vectors with assigned cluster IDs
 *     - Output: rows annotated with cluster IDs and optional data columns
 *     - Prefix: records the (prefix key, parent cluster ID) mapping
 */

class TPrefixKMeansScan: public TActor<TPrefixKMeansScan>, public NTable::IScan {
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
    TBufferData* PrefixBuf = nullptr;

    const ui32 Dimensions = 0;
    const ui32 K = 0;
    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;

    const TIndexBuildScanSettings ScanSettings;

    NTable::TTag EmbeddingTag;
    TTags ScanTags;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvPrefixKMeansResponse> Response;

    // FIXME: save PrefixRows as std::vector<std::pair<TSerializedCellVec, TSerializedCellVec>> to avoid parsing
    const ui32 PrefixColumns;
    // for PrefixKMeans, original table's primary key columns are passed separately,
    // because the prefix table contains them in a different order if they are both in PK and in the prefix
    const ui32 DataColumnCount;
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

    TPrefixKMeansScan(ui64 tabletId, const TUserTable& table, const NKikimrTxDataShard::TEvPrefixKMeansRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvPrefixKMeansResponse>&& response,
        std::unique_ptr<IClusters>&& clusters)
        : TActor{&TThis::StateWork}
        , Parent{request.GetChild()}
        , Child{Parent + 1}
        , State{EState::SAMPLE}
        , UploadState{request.GetUpload()}
        , Sampler(request.GetK(), request.GetSeed())
        , TabletId(tabletId)
        , BuildId{request.GetId()}
        , Uploader(request.GetScanSettings())
        , Dimensions(request.GetSettings().vector_dimension())
        , K(request.GetK())
        , ScanSettings(request.GetScanSettings())
        , ResponseActorId{responseActorId}
        , Response{std::move(response)}
        , PrefixColumns{request.GetPrefixColumns()}
        , DataColumnCount{(ui32)request.GetDataColumns().size()}
        , Clusters(std::move(clusters))
    {
        LOG_I("Create " << Debug());

        const auto& embedding = request.GetEmbeddingColumn();
        TVector<TString> data{request.GetDataColumns().begin(), request.GetDataColumns().end()};
        for (auto & col: request.GetSourcePrimaryKeyColumns()) {
            data.push_back(col);
        }
        ScanTags = MakeScanTags(table, embedding, {data.begin(), data.end()}, EmbeddingPos, DataPos, EmbeddingTag);
        Lead.To(ScanTags, {}, NTable::ESeek::Lower);
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
        {
            auto outputTypes = MakeOutputTypes(table, UploadState, embedding,
                {data.begin(), data.begin()+request.GetDataColumns().size()}, request.GetSourcePrimaryKeyColumns());
            OutputBuf = Uploader.AddDestination(request.GetOutputName(), outputTypes);
        }
        {
            auto types = GetAllTypes(table);

            auto prefixTypes = std::make_shared<NTxProxy::TUploadTypes>();
            prefixTypes->reserve(1 + PrefixColumns);

            Ydb::Type type;
            type.set_type_id(NTableIndex::ClusterIdType);
            prefixTypes->emplace_back(NTableIndex::NTableVectorKmeansTreeIndex::IdColumn, type);

            auto addType = [&](const auto& column) {
                auto it = types.find(column);
                if (it != types.end()) {
                    NScheme::ProtoFromTypeInfo(it->second, type);
                    prefixTypes->emplace_back(it->first, type);
                    types.erase(it);
                }
            };
            for (const auto& column : table.KeyColumnIds | std::views::take(PrefixColumns)) {
                addType(table.Columns.at(column).Name);
            }

            PrefixBuf = Uploader.AddDestination(request.GetPrefixName(), std::move(prefixTypes));
        }
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) noexcept final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        LOG_I("Prepare " << Debug());

        Driver = driver;
        Uploader.SetOwner(SelfId());

        return {EScan::Feed, {}};
    }

    TAutoPtr<IDestructable> Finish(EAbort abort) noexcept final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());

        Uploader.Finish(record, abort);

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

    EScan PageFault() noexcept final
    {
        LOG_T("PageFault " << Debug());
        return EScan::Feed;
    }

    EScan Seek(TLead& lead, ui64 seq) noexcept final
    {
    try {
        LOG_T("Seek " << seq << " " << Debug());

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead = Lead;

        return EScan::Feed;
    } catch (const std::exception& exc) {
        Uploader.AddIssue(exc);
        return EScan::Final;
    }
    }

    EScan Feed(TArrayRef<const TCell> key, const TRow& row) noexcept final
    {
    try {
        // LOG_T("Feed " << Debug());

        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *row);

        if (Prefix && !TCellVectorsEquals{}(Prefix.GetCells(), key.subspan(0, PrefixColumns))) {
            if (!FinishPrefix()) {
                // scan current prefix rows with a new state again
                return EScan::Reset;
            }
        }

        if (!Prefix) {
            Prefix = TSerializedCellVec{key.subspan(0, PrefixColumns)};

            // write {Prefix..., Parent} row to PrefixBuf:
            TVector<TCell> pk(::Reserve(Prefix.GetCells().size() + 1));
            pk.insert(pk.end(), Prefix.GetCells().begin(), Prefix.GetCells().end());
            pk.push_back(TCell::Make(Parent));

            PrefixBuf->AddRow(pk, {});
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
    } catch (const std::exception& exc) {
        Uploader.AddIssue(exc);
        return EScan::Final;
    }
    }

    EScan Exhausted() noexcept final
    {
    try {
        LOG_T("Exhausted " << Debug());

        if (!FinishPrefix()) {
            return EScan::Reset;
        }

        IsExhausted = true;

        // call Seek to wait uploads
        return EScan::Reset;
    } catch (const std::exception& exc) {
        Uploader.AddIssue(exc);
        return EScan::Final;
    }
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

    void StartNewPrefix() {
        Parent = Child + K;
        Child = Parent + 1;
        State = EState::SAMPLE;
        Lead.To(Prefix.GetCells(), NTable::ESeek::Upper); // seek to (prefix, inf)
        Prefix = {};
        IsFirstPrefixFeed = true;
        IsPrefixRowsValid = true;
        PrefixRows.Clear();
        Sampler.Finish();
        Clusters->Clear();
    }

    TString Debug() const
    {
        return TStringBuilder() << "TPrefixKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " State: " << State
            << " Parent: " << Parent << " Child: " << Child
            << " " << Sampler.Debug()
            << " " << Clusters->Debug()
            << " " << Uploader.Debug();
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

    void FeedBuildToBuild(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, row.Slice(DataPos+DataColumnCount), row.Slice(0, DataPos+DataColumnCount), key, false);
        }
    }

    void FeedBuildToPosting(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        if (auto pos = Clusters->FindCluster(row, EmbeddingPos); pos) {
            AddRowToData(*OutputBuf, Child + *pos, row.Slice(DataPos+DataColumnCount), row.Slice(DataPos, DataColumnCount), key, true);
        }
    }

    void FormLevelRows()
    {
        const bool isPostingLevel = UploadState == NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING;

        for (NTable::TPos pos = 0; const auto& row : Clusters->GetClusters()) {
            AddRowToLevel(*LevelBuf, Parent, Child + pos, row, isPostingLevel);
            ++pos;
        }
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
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    TRowVersion rowVersion = GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvPrefixKMeansResponse>();
        response->Record.SetId(id);
        response->Record.SetTabletId(TabletID());
        response->Record.SetRequestSeqNoGeneration(seqNo.Generation);
        response->Record.SetRequestSeqNoRound(seqNo.Round);

        LOG_N("Starting TPrefixKMeansScan TabletId: " << TabletID()
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
                LOG_E("Rejecting TPrefixKMeansScan bad request TabletId: " << TabletID()
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
        if (request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
            && request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING)
        {
            badRequest("Wrong upload");
        }

        if (request.GetK() < 2) {
            badRequest("Should be requested partition on at least two rows");
        }

        if (!request.HasLevelName()) {
            badRequest(TStringBuilder() << "Empty level table name");
        }
        if (!request.HasOutputName()) {
            badRequest(TStringBuilder() << "Empty output table name");
        }
        if (!request.HasPrefixName()) {
            badRequest(TStringBuilder() << "Empty prefix table name");
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
        if (request.GetPrefixColumns() <= 0) {
            badRequest("Should be requested on at least one prefix column");
        }
        if (request.GetPrefixColumns() > userTable.KeyColumnIds.size()) {
            badRequest(TStringBuilder() << "Should not be requested on more than " << userTable.KeyColumnIds.size() << " prefix columns");
        }
        if (request.GetSourcePrimaryKeyColumns().size() == 0) {
            badRequest("Request should include source primary key columns");
        }
        for (auto pkColumn : request.GetSourcePrimaryKeyColumns()) {
            if (!tags.contains(pkColumn)) {
                badRequest(TStringBuilder() << "Unknown source primary key column: " << pkColumn);
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
        TAutoPtr<NTable::IScan> scan = new TPrefixKMeansScan(
            TabletID(), userTable, request, ev->Sender, std::move(response), std::move(clusters)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvPrefixKMeansResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TPrefixKMeansScan");
    }
}

}
