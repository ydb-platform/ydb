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
 * TFilterKMeansScan copies rows from an intermediate build table with more than
 * <OverlapClusters> overlapping clusters to another build or posting table with
 * UP TO <OverlapClusters> overlapping clusters.
 * This is needed because, when overlapping clusters in the index are enabled,
 * at levels > 1, rows are first added to <OverlapClusters^2> nearest clusters,
 * and we need to select only <OverlapClusters> of them.
 *
 * This scan takes a BUILD table and writes output to BUILD or POSTING table.
 * (!) Please note that the scan uses the source table in a different form than other scans:
 * __ydb_parent column is expected to be in the end of its primary key rather than
 * the beginning.
 *
 * Source columns: <PK columns>, __ydb_parent, __ydb_foreign, __ydb_distance, <data columns>
 * Build destination columns: __ydb_parent, <PK columns>, __ydb_foreign, <data columns in the same order>
 * Posting destination columns: __ydb_parent, <PK columns>, <data columns in the same order>
 *
 * Request:
 * - The client sends TEvFilterKMeansRequest with:
 *   - Upload mode (MAIN_TO_BUILD, MAIN_TO_POSTING, BUILD_TO_BUILD or BUILD_TO_POSTING)
 *     determining input and output layouts
 *   - Name of the target table for row results ("posting" or "build")
 *   - SkipFirstKey, SkipLastKey
 *
 * Execution Flow:
 * - TFilterKMeansScan scans the whole input shard
 * - If SkipFirstKey is specified in the request:
 *   - Rows with <PK columns> equal to the beginning of the table range are copied to the response protobuf.
 * - If SkipLastKey is specified in the request:
 *   - Rows with <PK columns> equal to the end of the table range are copied to the response protobuf.
 * - For all other input rows, namely, for every primary key combination:
 *   - Top <OverlapClusters> rows with minimal __ydb_distance are selected, except
 *     rows with __ydb_distance[i] larger than __ydb_distance[0]*OverlapRatio
 *   - Selected rows are copied to the output table
 */

class TFilterKMeansScan: public TActor<TFilterKMeansScan>, public IActorExceptionHandler, public NTable::IScan {
protected:
    using EState = NKikimrTxDataShard::EKMeansState;

    EState UploadState;

    IDriver* Driver = nullptr;

    TLead Lead;

    ui64 TabletId = 0;
    ui64 BuildId = 0;

    ui64 ReadRows = 0;
    ui64 ReadBytes = 0;

    TBatchRowsUploader Uploader;

    TBufferData* OutputBuf = nullptr;

    const ui32 OverlapClusters = 0;
    const double OverlapRatio = 0;
    bool OutForeign = false;
    NTable::TPos DistancePos = 0;
    NTable::TPos DataPos = 0;

    TSerializedCellVec FirstIndexKey;
    TSerializedCellVec LastIndexKey;
    TVector<TSerializedCellVec> FirstKeyRows;
    TVector<TSerializedCellVec> LastKeyRows;
    TVector<TSerializedCellVec> LastRows;
    ui32 KeyColumnCount = 0;

    ui32 RetryCount = 0;

    const TIndexBuildScanSettings ScanSettings;
    bool SkipFirstKey = false;
    bool SkipLastKey = false;

    TTags ScanTags;

    TUploadStatus UploadStatus;

    TActorId ResponseActorId;
    TAutoPtr<TEvDataShard::TEvFilterKMeansResponse> Response;

    bool IsExhausted = false;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType()
    {
        return NKikimrServices::TActivity::FILTER_KMEANS_SCAN_ACTOR;
    }

    TFilterKMeansScan(ui64 tabletId, const TUserTable& table,
        const NKikimrTxDataShard::TEvFilterKMeansRequest& request,
        const TActorId& responseActorId, TAutoPtr<TEvDataShard::TEvFilterKMeansResponse>&& response)
        : TActor(&TThis::StateWork)
        , UploadState(request.GetUpload())
        , TabletId(tabletId)
        , BuildId(request.GetId())
        , Uploader(request.GetScanSettings())
        , OverlapClusters(request.GetOverlapClusters() ? request.GetOverlapClusters() : 1)
        , OverlapRatio(request.GetOverlapRatio())
        , ScanSettings(request.GetScanSettings())
        , SkipFirstKey(request.GetSkipFirstKey())
        , SkipLastKey(request.GetSkipLastKey())
        , ResponseActorId(responseActorId)
        , Response(std::move(response))
    {
        LOG_I("Create " << Debug());

        OutForeign = (request.GetUpload() == NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD);

        TTags scanTags;
        scanTags.push_back(UINT32_MAX); // Distance column will be put here
        DataPos = 1;

        auto outputTypes = std::make_shared<NTxProxy::TUploadTypes>();
        Ydb::Type type;
        {
            type.set_type_id(NTableIndex::NKMeans::ClusterIdType);
            outputTypes->emplace_back(NTableIndex::NKMeans::ParentColumn, type);
        }

        for (const auto& it : table.Columns) {
            if (it.second.IsKey) {
                KeyColumnCount++;
                if (it.second.Name != NTableIndex::NKMeans::ParentColumn) {
                    NScheme::ProtoFromTypeInfo(it.second.Type, type);
                    outputTypes->emplace_back(it.second.Name, type);
                }
            }
        }

        for (const auto& it : table.Columns) {
            if (it.second.IsKey) {
                continue;
            } else if (it.second.Name == NTableIndex::NKMeans::DistanceColumn) {
                // The only special column we need is __ydb_distance
                scanTags[0] = it.first;
                DistancePos = 0;
            } else if (it.second.Name == NTableIndex::NKMeans::IsForeignColumn) {
                // __ydb_foreign is copied to data, but only in BUILD_TO_BUILD mode
                if (OutForeign) {
                    scanTags.push_back(it.first);
                    NScheme::ProtoFromTypeInfo(it.second.Type, type);
                    outputTypes->emplace_back(it.second.Name, type);
                }
            } else {
                // All other columns are simply copied to data
                scanTags.push_back(it.first);
                NScheme::ProtoFromTypeInfo(it.second.Type, type);
                outputTypes->emplace_back(it.second.Name, type);
            }
        }

        Lead.To({}, NTable::ESeek::Lower);
        Lead.SetTags(scanTags);
        OutputBuf = Uploader.AddDestination(request.GetOutputName(), outputTypes);
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

        for (auto& row: FirstKeyRows) {
            record.AddFirstKeyRows(TSerializedCellVec::Serialize(row.GetCells()));
        }
        for (auto& row: LastKeyRows) {
            record.AddLastKeyRows(TSerializedCellVec::Serialize(row.GetCells()));
        }

        Uploader.Finish(record, status);

        if (Response->Record.GetStatus() == NKikimrIndexBuilder::DONE) {
            LOG_N("Done " << Debug() << " " << ToShortDebugString(Response->Record));
        } else {
            LOG_E("Failed " << Debug() << " " << ToShortDebugString(Response->Record));
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

        Feed(key, *row);

        return Uploader.ShouldWaitUpload() ? EScan::Sleep : EScan::Feed;
    }

    EScan Exhausted() final
    {
        LOG_T("Exhausted " << Debug());

        IsExhausted = true;
        if (LastIndexKey) {
            FinishKey(true);
        }

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

    TString Debug() const
    {
        return TStringBuilder() << "TFilterKMeansScan TabletId: " << TabletId << " Id: " << BuildId
            << " " << Uploader.Debug();
    }

    void Feed(TArrayRef<const TCell> key, TArrayRef<const TCell> row)
    {
        TVector<TCell> fullRow(key.begin(), key.end());
        fullRow.insert(fullRow.end(), row.begin(), row.end());
        auto pk = key.Slice(0, key.size()-1);
        if (SkipFirstKey) {
            if (!FirstIndexKey) {
                FirstIndexKey = TSerializedCellVec(pk);
                FirstKeyRows.push_back(TSerializedCellVec(fullRow));
                return;
            }
            if (TCellVectorsEquals{}(pk, FirstIndexKey.GetCells())) {
                FirstKeyRows.push_back(TSerializedCellVec(fullRow));
                return;
            } else {
                // first key skipping is finished
                SkipFirstKey = false;
                FirstIndexKey = {};
            }
        }
        if (LastIndexKey && !TCellVectorsEquals{}(pk, LastIndexKey.GetCells())) {
            FinishKey(false);
        }
        if (!LastIndexKey) {
            LastIndexKey = TSerializedCellVec(pk);
        }
        LastRows.push_back(TSerializedCellVec(fullRow));
    }

    void FinishKey(bool last)
    {
        if (last && SkipLastKey) {
            LastKeyRows = std::move(LastRows);
        } else if (LastRows.size() > 0) {
            NKikimr::NKMeans::FilterOverlapRows(LastRows, KeyColumnCount+DistancePos, OverlapClusters, OverlapRatio);
            TVector<TCell> pk(KeyColumnCount);
            for (auto& row: LastRows) {
                const auto& cells = row.GetCells();
                auto origKey = cells.Slice(0, KeyColumnCount);
                pk[0] = origKey.at(KeyColumnCount-1);
                for (size_t i = 0; i < KeyColumnCount-1; i++) {
                    pk[i+1] = origKey.at(i);
                }
                OutputBuf->AddRow(pk, cells.Slice(KeyColumnCount+DataPos), origKey);
            }
        }
        LastIndexKey = {};
        LastRows.clear();
    }
};

class TDataShard::TTxHandleSafeFilterKMeansScan final: public NTabletFlatExecutor::TTransactionBase<TDataShard> {
public:
    TTxHandleSafeFilterKMeansScan(TDataShard* self, TEvDataShard::TEvFilterKMeansRequest::TPtr&& ev)
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
    TEvDataShard::TEvFilterKMeansRequest::TPtr Ev;
};

void TDataShard::Handle(TEvDataShard::TEvFilterKMeansRequest::TPtr& ev, const TActorContext&)
{
    Execute(new TTxHandleSafeFilterKMeansScan(this, std::move(ev)));
}

void TDataShard::HandleSafe(TEvDataShard::TEvFilterKMeansRequest::TPtr& ev, const TActorContext& ctx)
{
    auto& request = ev->Get()->Record;
    const ui64 id = request.GetId();
    auto rowVersion = request.HasSnapshotStep() || request.HasSnapshotTxId()
        ? TRowVersion(request.GetSnapshotStep(), request.GetSnapshotTxId())
        : GetMvccTxVersion(EMvccTxMode::ReadOnly);
    TScanRecord::TSeqNo seqNo = {request.GetSeqNoGeneration(), request.GetSeqNoRound()};

    try {
        auto response = MakeHolder<TEvDataShard::TEvFilterKMeansResponse>();
        FillScanResponseCommonFields(*response, id, TabletID(), seqNo);

        LOG_N("Starting TFilterKMeansScan TabletId: " << TabletID()
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
                LOG_E("Rejecting TFilterKMeansScan bad request TabletId: " << TabletID()
                    << " " << request.ShortDebugString()
                    << " with response " << ToShortDebugString(response->Record));
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

        if (request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_BUILD
            && request.GetUpload() != NKikimrTxDataShard::UPLOAD_BUILD_TO_POSTING)
        {
            badRequest("Wrong upload");
        }

        if (request.GetOverlapClusters() <= 1) {
            badRequest("OverlapClusters should be > 1");
        }
        if (request.GetOverlapRatio() < 0) {
            badRequest("OverlapRatio should be >= 0");
        }

        if (!request.GetOutputName()) {
            badRequest(TStringBuilder() << "Empty output table name");
        }

        if (trySendBadRequest()) {
            return;
        }

        TAutoPtr<NTable::IScan> scan = new TFilterKMeansScan(
            TabletID(), userTable, request, ev->Sender, std::move(response)
        );

        StartScan(this, std::move(scan), id, seqNo, rowVersion, userTable.LocalTid);
    } catch (const std::exception& exc) {
        FailScan<TEvDataShard::TEvFilterKMeansResponse>(id, TabletID(), ev->Sender, seqNo, exc, "TFilterKMeansScan");
    }
}

}
