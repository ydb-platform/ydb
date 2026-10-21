#include "common_helper.h"
#include "kmeans_helper.h"
#include "vector_resolve_actor.h"

#include "../buffer_data.h"
#include "../datashard_impl.h"
#include "../range_ops.h"
#include "../scan_common.h"
#include "../upload_stats.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/kmeans_clusters.h>
#include <ydb/core/base/table_index.h>
#include <ydb/core/scheme/scheme_tablecell.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tablet_flat/flat_scan_delta.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <util/generic/algorithm.h>

#include <algorithm>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BUILD_INDEX

namespace NKikimr::NDataShard {
using namespace NKMeans;

/*
 * TDeltaKMeansScan applies the changes of the indexed table made between two
 * snapshots to a vector index that is already built for the older snapshot.
 *
 * The scan is an NTable::TDeltaScan over the main table, so it is fed only the
 * keys whose visible image differs between From and To, together with the old
 * and the new row images. Every changed key is resolved into posting clusters
 * through NBuildIndex::TVectorResolveActor (which uses the already built level
 * tables), and the resulting rows go to the posting table:
 *   - posting rows of the old image that are not a part of the new image are
 *     erased;
 *   - posting rows of the new image are upserted.
 *
 * Both the uploads and the erases are written to the shadow copy of the posting
 * table, the same way as the regular KMeans upload scans do.
 *
 * For a prefixed index the resolver may also assign ids to prefixes that did not
 * exist in the prefix table yet (the ids come from the prefix sequence and have
 * the posting parent flag set). Such prefixes are returned in the resolve
 * response and inserted into the prefix table.
 *
 * Note: two shards processing deltas with the same new prefix may assign it two
 * different ids. This is the known prefixed index build race; the duplicate
 * prefix rows are keyed by (prefix, __ydb_id), so they are harmless duplicates
 * until the prefix table key becomes just the prefix.
 */

class TDeltaKMeansScan: public TActor<TDeltaKMeansScan>, public IActorExceptionHandler, public NTable::TDeltaScan {
protected:
    using TBaseScan = NTable::TDeltaScan;

    // One changed table row with the images visible at From and at To.
    struct TRowDelta {
        TOwnedCellVec Key;
        bool HasOld = false;
        bool HasNew = false;
        TString OldEmbedding;
        TString NewEmbedding;
        TSerializedCellVec OldPrefix;
        TSerializedCellVec NewPrefix;
        TOwnedCellVec NewData;
        TVector<TClusterId> OldClusters;
        TVector<TClusterId> NewClusters;
    };

    // Position of a vector in the in-flight resolve request.
    struct TResolveRef {
        size_t Row = 0;
        bool IsOld = false;
    };

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;
    TString FatalError;

    TVector<NScheme::TTypeInfo> KeyTypes;
    TSerializedCellVec LastAckedKey;
    ui64 NextCheckpointAtBytes = 0;

    ui32 PrefixColumns = 0;
    TVector<TString> PrefixColumnNames;
    TVector<NTable::TPos> PrefixPositions;
    ui32 DataColumnsCount = 0;
    NTable::TPos EmbeddingPos = 0;
    NTable::TPos DataPos = 1;
    const ui32 OverlapClusters = 0;
    const double OverlapRatio = 0;

    const TIndexBuildScanSettings ScanSettings;
    TTags ScanTags;

    TBatchRowsUploader Uploader;
    TBufferData* PostingBuf = nullptr;
    TBufferData* PrefixBuf = nullptr;
    TBatchRowsUploader::TEraseBuffer* PostingEraseBuf = nullptr;

    std::unique_ptr<IClusters> Clusters;

    NBuildIndex::TVectorResolveSettings ResolverSettings;
    TActorId ResolverId;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvDeltaKMeansResponse> Response;

    TVector<TRowDelta> Pending;
    ui64 PendingBytes = 0;
    TVector<TResolveRef> ResolveRefs;
    ui64 ResolveRequestId = 0;
    bool ResolveInFlight = false;
    // Prefixes this scan has already generated ids for. Prevents issuing several
    // ids for one prefix within this scan while the inserted prefixes are not
    // visible in the prefix table yet.
    THashMap<TString, TClusterId> LocalPrefixes;

    bool IsExhausted = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::DELTA_KMEANS_SCAN_ACTOR;
    }

    TDeltaKMeansScan(ui64 tabletId, const TUserTable& table,
        const NKikimrTxDataShard::TEvDeltaKMeansRequest& request,
        const TRowVersion& from, const TRowVersion& to,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvDeltaKMeansResponse>&& response,
        std::unique_ptr<IClusters>&& clusters)
        : TActor(&TThis::StateWork)
        , TBaseScan(from, to)
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , KeyTypes(table.KeyColumnTypes)
        , PrefixColumns(request.GetPrefixColumns())
        , OverlapClusters(request.GetOverlapClusters() ? request.GetOverlapClusters() : 1)
        , OverlapRatio(request.GetOverlapRatio())
        , ScanSettings(request.GetScanSettings())
        , Uploader(request.GetDatabaseName(), request.GetScanSettings())
        , Clusters(std::move(clusters))
        , ResponseActorId(responseActorId)
        , Response(std::move(response))
    {
        NextCheckpointAtBytes = ScanSettings.GetMaxCheckpointBytes();

        PrefixColumnNames.assign(request.GetPrefixColumnNames().begin(), request.GetPrefixColumnNames().end());
        if (PrefixColumnNames.empty() && PrefixColumns > 0) {
            // Backward compatibility for requests that only carry the column count.
            for (ui32 i = 0; i < PrefixColumns; ++i) {
                PrefixColumnNames.push_back(table.Columns.at(table.KeyColumnIds.at(i)).Name);
            }
        }
        PrefixColumns = PrefixColumnNames.size();
        DataColumnsCount = request.DataColumnsSize();

        const auto& embedding = request.GetEmbeddingColumn();
        ScanTags = MakeScanTags(table, embedding, request.GetDataColumns(), false /* forBuild */, EmbeddingPos, DataPos);

        if (PrefixColumns > 0) {
            auto allTags = GetAllTags(table);
            PrefixPositions.reserve(PrefixColumns);
            for (const auto& name : PrefixColumnNames) {
                const auto tag = allTags.at(name);
                if (auto it = std::find(ScanTags.begin(), ScanTags.end(), tag); it != ScanTags.end()) {
                    PrefixPositions.push_back(static_cast<NTable::TPos>(it - ScanTags.begin()));
                } else {
                    PrefixPositions.push_back(ScanTags.size());
                    ScanTags.push_back(tag);
                }
            }
        }

        Lead.To(ScanTags, {}, NTable::ESeek::Lower);
        if (request.HasKeyRange()) {
            TSerializedTableRange resumeRange;
            resumeRange.Load(request.GetKeyRange());
            auto scanRange = Intersect(KeyTypes, resumeRange.ToTableRange(), table.GetTableRange());
            Lead = CreateLeadFrom(scanRange);
            Lead.SetTags(ScanTags);
        }

        PostingBuf = Uploader.AddDestination(request.GetOutputName(),
            MakeOutputTypes(table, NKikimrTxDataShard::UPLOAD_MAIN_TO_POSTING, embedding, request.GetDataColumns(),
                {}, false /* withForeignFlag */));
        PostingEraseBuf = Uploader.AddEraseDestination(request.GetOutputName());
        if (PrefixColumns > 0) {
            PrefixBuf = Uploader.AddDestination(request.GetPrefixName(), MakePrefixTypes(table));
        }

        ResolverSettings.DatabaseName = request.GetDatabaseName();
        ResolverSettings.LevelTablePath = request.GetLevelName();
        ResolverSettings.PrefixTablePath = request.GetPrefixName();
        ResolverSettings.PrefixColumns = PrefixColumnNames;
        ResolverSettings.IndexSettings = request.GetSettings();
        ResolverSettings.OverlapClusters = OverlapClusters;
        ResolverSettings.OverlapRatio = OverlapRatio;

        YDB_LOG_INFO("Scan actor created",
            {"debug", Debug()});
    }

    TInitialState Prepare(IDriver* driver, TIntrusiveConstPtr<TScheme>) final
    {
        TActivationContext::AsActorContext().RegisterWithSameMailbox(this);
        Driver = driver;
        Uploader.SetOwner(SelfId());

        ResolverId = RegisterWithSameMailbox(NBuildIndex::CreateVectorResolveActor(std::move(ResolverSettings)));

        YDB_LOG_INFO("Scan actor prepared",
            {"debug", Debug()});

        TConf conf;
        conf.NoErased = false; // a version scan needs technical rows too
        return {EScan::Feed, conf};
    }

    TAutoPtr<IDestructable> Finish(const std::exception& exc) final
    {
        if (FatalError.empty()) {
            FatalError = exc.what();
        }
        return Finish(EStatus::Exception);
    }

    TAutoPtr<IDestructable> Finish(EStatus status) final
    {
        auto& record = Response->Record;
        record.MutableMeteringStats()->SetReadRows(ReadRows);
        record.MutableMeteringStats()->SetReadBytes(ReadBytes);
        if (Driver) {
            record.MutableMeteringStats()->SetCpuTimeUs(Driver->GetTotalCpuTimeUs());
        }

        if (LastAckedKey.GetBuffer()) {
            record.SetLastKeyAck(LastAckedKey.GetBuffer());
        }

        Uploader.Finish(record, status);

        if (!FatalError.empty()) {
            record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR);
            auto* issue = record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(FatalError);
        }

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            YDB_LOG_NOTICE("Scan completed successfully",
                {"debug", Debug()},
                {"responseRecord", Response->Record.ShortDebugString()});
        } else {
            YDB_LOG_ERROR("Scan failed",
                {"debug", Debug()},
                {"responseRecord", Response->Record.ShortDebugString()});
        }

        if (ResolverId) {
            Send(ResolverId, new TEvents::TEvPoison());
            ResolverId = {};
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
        return EScan::Feed;
    }

    EScan Seek(TLead& lead, ui64 seq) final
    {
        YDB_LOG_TRACE("Seek",
            {"seekSequence", seq},
            {"debug", Debug()});

        if (IsExhausted) {
            return Uploader.CanFinish()
                ? EScan::Final
                : EScan::Sleep;
        }

        lead = Lead;

        return EScan::Feed;
    }

    EScan Exhausted() final
    {
        YDB_LOG_TRACE("Scan range exhausted",
            {"debug", Debug()});

        IsExhausted = true;

        if (!Pending.empty() && StartResolve()) {
            return EScan::Sleep;
        }

        // call Seek to wait uploads and to finish
        return EScan::Reset;
    }

protected:
    STFUNC(StateWork)
    {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvTxUserProxy::TEvUploadRowsResponse, HandleUpload);
            HFunc(TEvTxUserProxy::TEvEraseRowsResponse, HandleErase);
            HFunc(NBuildIndex::TEvVectorResolveResponse, HandleResolve);
            CFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                YDB_LOG_ERROR("Unexpected event in scan actor",
                    {"eventType", ev->GetTypeRewrite()},
                    {"eventDetails", ev->ToString()},
                    {"debug", Debug()});
        }
    }

    void HandleWakeup(const NActors::TActorContext& /*ctx*/)
    {
        YDB_LOG_DEBUG("Retrying row upload",
            {"debug", Debug()});

        Uploader.RetryUpload();
    }

    void HandleUpload(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        HandleUploadResponse(Uploader.Handle(ev), ctx);
    }

    void HandleErase(TEvTxUserProxy::TEvEraseRowsResponse::TPtr& ev, const TActorContext& ctx)
    {
        HandleUploadResponse(Uploader.Handle(ev), ctx);
    }

    void HandleUploadResponse(bool batchUploaded, const TActorContext& ctx)
    {
        if (!Driver) {
            return;
        }

        if (!Uploader.GetUploadStatus().IsSuccess()) {
            if (auto retryAfter = Uploader.GetRetryAfter(); retryAfter) {
                YDB_LOG_NOTICE("Row upload failed with retriable error",
                    {"debug", Debug()},
                    {"uploadStatus", Uploader.GetUploadStatus()});
                ctx.Schedule(*retryAfter, new TEvents::TEvWakeup());
                return;
            }

            YDB_LOG_NOTICE("Row upload failed, aborting scan",
                {"debug", Debug()},
                {"uploadStatus", Uploader.GetUploadStatus()});

            Driver->Touch(EScan::Final);
            return;
        }

        if (batchUploaded) {
            if (auto flushed = Uploader.GetMinFlushedKey(); flushed && Uploader.GetUploadBytes() >= NextCheckpointAtBytes) {
                NextCheckpointAtBytes = Uploader.GetUploadBytes() + ScanSettings.GetMaxCheckpointBytes();
                LastAckedKey = std::move(*flushed);

                auto progress = MakeHolder<TEvDataShard::TEvDeltaKMeansResponse>();
                auto& rec = progress->Record;
                rec.SetId(BuildId);
                rec.SetTabletId(TabletId);
                rec.SetRequestSeqNoGeneration(Response->Record.GetRequestSeqNoGeneration());
                rec.SetRequestSeqNoRound(Response->Record.GetRequestSeqNoRound());
                rec.SetStatus(NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS);
                rec.SetLastKeyAck(LastAckedKey.GetBuffer());
                Send(ResponseActorId, progress.Release());
            }
        }

        if (!ResolveInFlight) {
            Driver->Touch(EScan::Feed);
        }
    }

    EScan FeedDelta(TArrayRef<const TCell> key, const NTable::TRowState& oldState, const NTable::TRowState& newState) final
    {
        ++ReadRows;
        ReadBytes += CountRowCellBytes(key, *oldState) + CountRowCellBytes(key, *newState);

        TRowDelta row;
        row.Key = TOwnedCellVec::Make(key);

        const auto copyPrefix = [&](const NTable::TRowState& state) {
            TVector<TCell> cells;
            cells.reserve(PrefixPositions.size());
            for (const auto pos : PrefixPositions) {
                cells.push_back(state.Get(pos));
            }
            return TSerializedCellVec(cells);
        };

        if (oldState.GetRowState() != NTable::ERowOp::Absent) {
            row.HasOld = true;
            if (!CopyEmbedding(oldState, row.OldEmbedding)) {
                return EScan::Final;
            }
            if (PrefixColumns > 0) {
                row.OldPrefix = copyPrefix(oldState);
            }
        }

        if (newState.GetRowState() != NTable::ERowOp::Absent) {
            row.HasNew = true;
            if (!CopyEmbedding(newState, row.NewEmbedding)) {
                return EScan::Final;
            }
            if (PrefixColumns > 0) {
                row.NewPrefix = copyPrefix(newState);
            }

            TVector<TCell> data;
            data.reserve(DataColumnsCount);
            for (NTable::TPos pos = DataPos; pos < DataPos + DataColumnsCount; ++pos) {
                data.push_back(newState.Get(pos));
            }
            row.NewData = TOwnedCellVec::Make(data);
        }

        PendingBytes += row.Key.DataSize() + row.OldEmbedding.size() + row.NewEmbedding.size() + row.NewData.DataSize()
            + row.OldPrefix.GetBuffer().size() + row.NewPrefix.GetBuffer().size();
        Pending.push_back(std::move(row));

        if (Pending.size() > ScanSettings.GetMaxBatchRows() || PendingBytes > ScanSettings.GetMaxBatchBytes()) {
            if (StartResolve()) {
                return EScan::Sleep;
            }
        }

        return EScan::Feed;
    }

    void HandleResolve(NBuildIndex::TEvVectorResolveResponse::TPtr& ev, const TActorContext& /*ctx*/)
    {
        if (!ResolveInFlight || ev->Get()->RequestId != ResolveRequestId) {
            return;
        }
        ResolveInFlight = false;

        auto* response = ev->Get();
        if (!response->Success) {
            FatalError = response->Issues.ToOneLineString();
            if (FatalError.empty()) {
                FatalError = "Failed to resolve vectors into clusters";
            }
            if (Driver) {
                Driver->Touch(EScan::Final);
            }
            return;
        }

        if (response->Clusters.size() != ResolveRefs.size()) {
            FatalError = TStringBuilder() << "Resolve response has " << response->Clusters.size()
                << " vectors, expected " << ResolveRefs.size();
            if (Driver) {
                Driver->Touch(EScan::Final);
            }
            return;
        }

        for (size_t pos = 0; pos < ResolveRefs.size(); ++pos) {
            const auto& ref = ResolveRefs[pos];
            auto& row = Pending[ref.Row];
            auto& clusters = ref.IsOld ? row.OldClusters : row.NewClusters;
            clusters = std::move(response->Clusters[pos]);
        }

        for (auto& newPrefix : response->NewPrefixes) {
            // Already inserted by this scan: do not duplicate the prefix row.
            if (LocalPrefixes.contains(newPrefix.Prefix.GetBuffer())) {
                continue;
            }
            LocalPrefixes.emplace(newPrefix.Prefix.GetBuffer(), newPrefix.Id);

            TVector<TCell> pk(newPrefix.Prefix.GetCells().begin(), newPrefix.Prefix.GetCells().end());
            pk.push_back(TCell::Make(newPrefix.Id));
            PrefixBuf->AddRow(pk, {});
        }

        ApplyPending();

        if (!Driver) {
            return;
        }
        if (!Uploader.ShouldWaitUpload()) {
            Driver->Touch(EScan::Feed);
        }
    }

    TString Debug() const
    {
        return TStringBuilder() << "TDeltaKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " From: " << GetFrom() << " To: " << GetTo()
            << " " << Uploader.Debug();
    }

private:
    bool CopyEmbedding(const NTable::TRowState& state, TString& out)
    {
        const TCell& cell = state.Get(EmbeddingPos);
        if (cell.IsNull() || cell.Size() == 0) {
            return true;
        }

        const auto embedding = cell.AsRef();
        if (!Clusters->IsExpectedFormat(embedding)) {
            FatalError = Clusters->FormatError(embedding);
            return false;
        }

        out.assign(embedding.data(), embedding.size());
        return true;
    }

    // Sends the pending rows to the resolver. Returns false when nothing has to
    // be resolved asynchronously (the rows are already applied in that case).
    bool StartResolve()
    {
        if (Pending.empty()) {
            return false;
        }
        if (ResolveInFlight) {
            return true;
        }

        ResolveRefs.clear();
        ResolveRefs.reserve(Pending.size() * 2);

        auto request = MakeHolder<NBuildIndex::TEvVectorResolveRequest>();
        request->RequestId = ++ResolveRequestId;

        for (size_t i = 0; i < Pending.size(); ++i) {
            auto& row = Pending[i];
            row.OldClusters.clear();
            row.NewClusters.clear();

            if (row.HasOld && !row.OldEmbedding.empty()) {
                const TClusterId* localPrefix = PrefixColumns > 0
                    ? LocalPrefixes.FindPtr(row.OldPrefix.GetBuffer())
                    : nullptr;
                if (localPrefix) {
                    // A prefix created by this very scan is a single posting parent.
                    row.OldClusters.push_back(*localPrefix);
                } else {
                    ResolveRefs.push_back({i, true});
                    request->Vectors.push_back(std::move(row.OldEmbedding));
                    if (PrefixColumns > 0) {
                        request->Prefixes.push_back(row.OldPrefix);
                    }
                }
            }

            if (row.HasNew && !row.NewEmbedding.empty()) {
                const TClusterId* localPrefix = PrefixColumns > 0
                    ? LocalPrefixes.FindPtr(row.NewPrefix.GetBuffer())
                    : nullptr;
                if (localPrefix) {
                    row.NewClusters.push_back(*localPrefix);
                } else {
                    ResolveRefs.push_back({i, false});
                    request->Vectors.push_back(std::move(row.NewEmbedding));
                    if (PrefixColumns > 0) {
                        request->Prefixes.push_back(row.NewPrefix);
                    }
                }
            }
        }

        if (request->Vectors.empty()) {
            ApplyPending();
            return false;
        }

        Y_ENSURE(ResolverId, "Resolver is not registered");
        Send(ResolverId, request.Release());
        ResolveInFlight = true;
        return true;
    }

    void ApplyPending()
    {
        for (auto& row : Pending) {
            ApplyRow(row);
        }
        Pending.clear();
        PendingBytes = 0;
        ResolveRefs.clear();
    }

    void ApplyRow(TRowDelta& row)
    {
        SortUnique(row.OldClusters);
        SortUnique(row.NewClusters);

        Uploader.SetCurrentSourceKey(TSerializedCellVec(row.Key));

        for (TClusterId cluster : row.OldClusters) {
            if (std::binary_search(row.NewClusters.begin(), row.NewClusters.end(), cluster)) {
                // The row stays in this cluster, the upsert below overwrites it.
                continue;
            }
            PostingEraseBuf->AddKey(PostingKey(cluster, row.Key));
        }

        for (TClusterId cluster : row.NewClusters) {
            // The resolver returns leaf cluster ids, which are posting parents already.
            TVector<TCell> pk(::Reserve(row.Key.size() + 1));
            pk.push_back(TCell::Make(cluster));
            pk.insert(pk.end(), row.Key.begin(), row.Key.end());
            PostingBuf->AddRow(pk, row.NewData, row.Key);
        }
    }

    static TVector<TCell> PostingKey(TClusterId cluster, TConstArrayRef<TCell> key)
    {
        Y_ENSURE(HasPostingParentFlag(cluster), "Cluster was not resolved to a posting parent");

        TVector<TCell> pk(::Reserve(key.size() + 1));
        pk.push_back(TCell::Make(cluster));
        pk.insert(pk.end(), key.begin(), key.end());
        return pk;
    }

    std::shared_ptr<NTxProxy::TUploadTypes> MakePrefixTypes(const TUserTable& table) const
    {
        auto types = GetAllTypes(table);

        auto result = std::make_shared<NTxProxy::TUploadTypes>();
        result->reserve(1 + PrefixColumns);

        Ydb::Type type;
        type.set_type_id(NTableIndex::NKMeans::ClusterIdType);
        result->emplace_back(NTableIndex::NKMeans::IdColumn, type);

        for (const auto& columnName : PrefixColumnNames) {
            auto it = types.find(columnName);
            if (it != types.end()) {
                NScheme::ProtoFromTypeInfo(it->second, type);
                result->emplace_back(it->first, type);
            }
        }

        return result;
    }
};

class TDataShard::TTxHandleSafeDeltaKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeDeltaKMeansScan(TDataShard* self, TEvDataShard::TEvDeltaKMeansRequest::TPtr&& ev)
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
    TEvDataShard::TEvDeltaKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvDeltaKMeansRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeDeltaKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvDeltaKMeansRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvDeltaKMeansResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

        auto badRequest = [&](const TString& error) {
            response->Record.SetStatus(NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST);
            auto issue = response->Record.AddIssues();
            issue->set_severity(NYql::TSeverityIds::S_ERROR);
            issue->set_message(error);
        };
        auto trySendBadRequest = [&] {
            if (response->Record.GetStatus() == NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST) {
                YDB_LOG_ERROR("Rejecting invalid delta K-means scan request",
                    {"tabletId", TabletID()},
                    {"request", request.ShortDebugString()},
                    {"responseRecord", response->Record.ShortDebugString()});
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

        // 2. Validating snapshots
        if (!request.HasFromSnapshotStep() || !request.HasFromSnapshotTxId()
            || !request.HasToSnapshotStep() || !request.HasToSnapshotTxId())
        {
            badRequest("Both From and To snapshots must be specified");
        }
        const TRowVersion from(request.GetFromSnapshotStep(), request.GetFromSnapshotTxId());
        const TRowVersion to(request.GetToSnapshotStep(), request.GetToSnapshotTxId());
        if (to <= from) {
            badRequest(TStringBuilder() << "To snapshot " << to << " must be newer than From snapshot " << from);
        }
        for (const TRowVersion& version : {from, to}) {
            if (version.Step == 0 || version.TxId == 0) {
                badRequest(TStringBuilder() << "Invalid row version " << version);
            } else if (!SnapshotManager.FindAvailable(TSnapshotKey(pathId, version.Step, version.TxId))) {
                badRequest(TStringBuilder() << "Unknown snapshot for path id " << pathId.OwnerId << ":" << pathId.LocalPathId
                    << ", snapshot step is " << version.Step << ", snapshot tx is " << version.TxId);
            }
        }

        // 3. Validating request fields
        if (request.GetLevelName().empty()) {
            badRequest("Empty level table name");
        }
        if (request.GetOutputName().empty()) {
            badRequest("Empty output table name");
        }

        const ui32 prefixColumns = request.GetPrefixColumns();
        const auto& prefixNames = request.GetPrefixColumnNames();

        auto tags = GetAllTags(userTable);

        if (prefixColumns > 0) {
            if (request.GetPrefixName().empty()) {
                badRequest("Empty prefix table name for a prefixed index");
            }
            if (!prefixNames.empty()) {
                if (static_cast<ui32>(prefixNames.size()) != prefixColumns) {
                    badRequest(TStringBuilder() << "Prefix has " << prefixColumns << " columns, but "
                        << prefixNames.size() << " column names were provided");
                }
                for (const auto& name : prefixNames) {
                    if (!tags.contains(name)) {
                        badRequest(TStringBuilder() << "Unknown prefix column: " << name);
                    }
                }
            } else if (prefixColumns > userTable.KeyColumnIds.size()) {
                badRequest(TStringBuilder() << "Prefix has " << prefixColumns << " columns, but table key has only "
                    << userTable.KeyColumnIds.size());
            }
        }

        if (!tags.contains(request.GetEmbeddingColumn())) {
            badRequest(TStringBuilder() << "Unknown embedding column: " << request.GetEmbeddingColumn());
        }
        for (const auto& dataColumn : request.GetDataColumns()) {
            if (!tags.contains(dataColumn)) {
                badRequest(TStringBuilder() << "Unknown data column: " << dataColumn);
            }
        }

        // 4. Validating vector index settings
        TString error;
        auto clusters = NKikimr::NKMeans::CreateClusters(request.GetSettings(), 0, error);
        if (!clusters) {
            badRequest(error);
        }

        if (trySendBadRequest()) {
            return;
        }

        YDB_LOG_NOTICE("Starting delta K-means scan",
            {"tabletId", TabletID()},
            {"request", request.ShortDebugString()},
            {"from", from},
            {"to", to});

        // Note: it's very unlikely that we have volatile txs before this snapshot
        if (VolatileTxManager.HasVolatileTxsAtSnapshot(to)) {
            VolatileTxManager.AttachWaitingSnapshotEvent(to, std::unique_ptr<IEventHandle>(ev.Release()));
            return;
        }

        TAutoPtr<NTable::IScan> scan = new TDeltaKMeansScan(
            TabletID(), userTable, request, from, to, ev->Sender, std::move(response), std::move(clusters)
        );

        StartScan(this, std::move(scan), id, seqNo, to, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvDeltaKMeansResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TDeltaKMeansScan");
    }
}

}


#undef YDB_LOG_THIS_FILE_COMPONENT
