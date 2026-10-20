#include "column_index_scan.h"

#include <ydb/core/kqp/compute_actor/kqp_compute_events.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/columnshard/backup/iscan/iscan.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/formats/arrow/arrow_helpers.h>

#include <util/generic/deque.h>
#include <util/generic/size_literals.h>

namespace NKikimr::NColumnShard {

namespace {

class TColumnTableIndexScan: public TActorBootstrapped<TColumnTableIndexScan> {
public:
    TColumnTableIndexScan(
        const TActorId& columnShardId,
        NKikimrTxDataShard::TEvBuildIndexCreateRequest request,
        TVector<TColumnIndexScanColumn> columns,
        ui32 keyColumnCount)
        : ColumnShardId(columnShardId)
        , Request(std::move(request))
        , Columns(std::move(columns))
        , KeyColumnCount(keyColumnCount)
    {}

    void Bootstrap() {
        Become(&TColumnTableIndexScan::StateWork);
        SendScan();
    }

private:
    void SendScan() {
        auto ev = std::make_unique<TEvDataShard::TEvKqpScan>();
        ev->Record.SetLocalPathId(Request.GetPathId());
        ev->Record.SetTxId(Request.GetSnapshotTxId());
        ev->Record.SetScanId(Request.GetId());
        ev->Record.MutableSnapshot()->SetStep(Request.GetSnapshotStep());
        ev->Record.MutableSnapshot()->SetTxId(Request.GetSnapshotTxId());
        ev->Record.SetDataFormat(NKikimrDataEvents::FORMAT_ARROW);
        for (const auto& column : Columns) {
            ev->Record.AddColumnTags(column.Id);
        }
        Send(ColumnShardId, ev.release());
    }

    void Stop() {
        if (ScanActorId) {
            Send(ScanActorId, new TEvents::TEvPoison);
            ScanActorId = {};
        }
        if (Uploader) {
            Send(Uploader, new TEvents::TEvPoison());
            Uploader = {};
        }
    }

    void Finish(NKikimrIndexBuilder::EBuildStatus status, const TString& error) {
        if (Finished) {
            return;
        }
        Finished = true;
        Stop();
        auto progress = MakeHolder<TEvDataShard::TEvBuildIndexProgressResponse>();
        progress->Record.SetId(Request.GetId());
        progress->Record.SetTabletId(Request.GetTabletId());
        progress->Record.SetOwnerId(Request.GetOwnerId());
        progress->Record.SetPathId(Request.GetPathId());
        progress->Record.SetRequestSeqNoGeneration(Request.GetSeqNoGeneration());
        progress->Record.SetRequestSeqNoRound(Request.GetSeqNoRound());
        progress->Record.SetStatus(status);
        if (error) {
            auto* issue = progress->Record.AddIssues();
            issue->set_message(error);
        }
        Send(ColumnShardId, progress.Release());
        PassAway();
    }

    void UploadRows(std::shared_ptr<NTxProxy::TUploadRows> rows) {
        TVector<std::pair<TString, NScheme::TTypeInfo>> ydbSchema;
        auto types = std::make_shared<NTxProxy::TUploadTypes>();
        ydbSchema.reserve(Columns.size());
        types->reserve(Columns.size());
        for (const auto& column : Columns) {
            ydbSchema.emplace_back(column.Name, column.Type);
            Ydb::Type type;
            NScheme::ProtoFromTypeInfo(column.Type, type);
            types->emplace_back(column.Name, std::move(type));
        }
        Types = std::move(types);
        YdbSchema = std::move(ydbSchema);

        Uploader = Register(NTxProxy::CreateUploadRowsInternal(
            SelfId(),
            Request.GetDatabaseName(),
            Request.GetTargetName(),
            Types,
            std::move(rows),
            NTxProxy::EUploadRowsMode::WriteToTableShadow,
            true,
            true));
    }

    std::shared_ptr<NTxProxy::TUploadRows> RowsFromBatch(const std::shared_ptr<arrow::RecordBatch>& batch) const {
        auto converted = NBackup::BatchToRows(batch, YdbSchema);
        if (converted.IsFail()) {
            return nullptr;
        }
        auto rows = std::make_shared<NTxProxy::TUploadRows>();
        for (auto& serialized : converted.DetachResult()) {
            const auto cells = serialized.GetCells();
            if (cells.size() < KeyColumnCount) {
                return nullptr;
            }
            rows->emplace_back(
                TSerializedCellVec(cells.Slice(0, KeyColumnCount)),
                TSerializedCellVec::Serialize(cells.Slice(KeyColumnCount)));
        }
        return rows;
    }

    void HandleInit(NKqp::TEvKqpCompute::TEvScanInitActor::TPtr& ev) {
        ScanActorId = NActors::ActorIdFromProto(ev->Get()->Record.GetScanActorId());
        Send(ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(8_MB, 0, 1));
    }

    void HandleScan(NKqp::TEvKqpCompute::TEvScanData::TPtr& ev) {
        if (Finished || Uploader) {
            Pending.push_back(std::move(ev));
            return;
        }
        const bool finished = ev->Get()->Finished;
        if (ev->Get()->ArrowBatch && ev->Get()->ArrowBatch->num_rows() > 0) {
            TVector<std::pair<TString, NScheme::TTypeInfo>> ydbSchema;
            ydbSchema.reserve(Columns.size());
            for (const auto& column : Columns) {
                ydbSchema.emplace_back(column.Name, column.Type);
            }
            YdbSchema = std::move(ydbSchema);
            auto batch = NArrow::ToBatch(ev->Get()->ArrowBatch);
            auto rows = RowsFromBatch(batch);
            if (!rows) {
                Finish(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR, "Failed to convert column batch into index rows");
                return;
            }
            if (!rows->empty()) {
                ScanFinished = finished;
                UploadRows(std::move(rows));
                return;
            }
        }
        if (finished) {
            Finish(NKikimrIndexBuilder::EBuildStatus::DONE, {});
            return;
        }
        Send(ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(8_MB, 0, 1));
    }

    void HandleScanError(NKqp::TEvKqpCompute::TEvScanError::TPtr& ev) {
        Finish(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR, ev->Get()->Record.ShortDebugString());
    }

    void HandleUndelivered(TEvents::TEvUndelivered::TPtr&) {
        Finish(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR, "Column index scan event was not delivered");
    }

    void HandleUpload(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        Uploader = {};
        if (ev->Get()->Status != Ydb::StatusIds::SUCCESS) {
            Finish(NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR, ev->Get()->Issues.ToString());
            return;
        }
        if (!Pending.empty()) {
            auto next = std::move(Pending.front());
            Pending.pop_front();
            HandleScan(next);
            return;
        }
        if (ScanFinished) {
            Finish(NKikimrIndexBuilder::EBuildStatus::DONE, {});
            return;
        }
        Send(ScanActorId, new NKqp::TEvKqpCompute::TEvScanDataAck(8_MB, 0, 1));
    }

    void HandlePoison() {
        if (Finished) {
            return;
        }
        Finished = true;
        Stop();
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        hFunc(NKqp::TEvKqpCompute::TEvScanInitActor, HandleInit)
        hFunc(NKqp::TEvKqpCompute::TEvScanData, HandleScan)
        hFunc(NKqp::TEvKqpCompute::TEvScanError, HandleScanError)
        hFunc(TEvents::TEvUndelivered, HandleUndelivered)
        hFunc(TEvTxUserProxy::TEvUploadRowsResponse, HandleUpload)
        cFunc(TEvents::TEvPoisonPill::EventType, HandlePoison)
    )

    const TActorId ColumnShardId;
    const NKikimrTxDataShard::TEvBuildIndexCreateRequest Request;
    const TVector<TColumnIndexScanColumn> Columns;
    const ui32 KeyColumnCount;

    TActorId ScanActorId;
    TActorId Uploader;
    TVector<std::pair<TString, NScheme::TTypeInfo>> YdbSchema;
    std::shared_ptr<NTxProxy::TUploadTypes> Types;
    TDeque<NKqp::TEvKqpCompute::TEvScanData::TPtr> Pending;
    bool ScanFinished = false;
    bool Finished = false;
};

} // namespace

NActors::IActor* CreateColumnTableIndexScan(
    const NActors::TActorId& columnShardId,
    NKikimrTxDataShard::TEvBuildIndexCreateRequest request,
    TVector<TColumnIndexScanColumn> columns,
    ui32 keyColumnCount)
{
    return new TColumnTableIndexScan(columnShardId, std::move(request), std::move(columns), keyColumnCount);
}

}
