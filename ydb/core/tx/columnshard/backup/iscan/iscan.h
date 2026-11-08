#pragma once

#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/tx/datashard/export_iface.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/conclusion/result.h>

namespace NKikimr::NColumnShard::NBackup {

class TRowWriter: public NArrow::IRowWriter {
public:
    explicit TRowWriter(TVector<TSerializedCellVec>& rows);
    void AddRow(const TConstArrayRef<TCell>& cells) override;

private:
    TVector<TSerializedCellVec>& Rows;
};

struct TExportDriver final: NTable::IDriver {
    TExportDriver(TActorSystem* actorSystem, const TActorId& subscriberActorId);

    virtual void Touch(NTable::EScan state) override;

    virtual void Throw(const std::exception& e) override;

    virtual ui64 GetTotalCpuTimeUs() const override;

private:
    NActors::TActorSystem* ActorSystem;
    TActorId SubscriberActorId;
};

TConclusion<TVector<TSerializedCellVec>> BatchToRows(
    const std::shared_ptr<arrow::RecordBatch>& batch, const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema);

// The parts of the data file sent by the export scan
struct TDataFileParts {
    // 0 means no limit
    ui32 Limit = 0;
    ui32 Sent = 0;
    // Rows collected since the last sent part
    ui64 BufferedRows = 0;

    // Only the last part of the data file is left: the buffered rows have to be sent as the last part
    bool IsLastPartLeft() const {
        return Limit && Sent + 1 >= Limit;
    }
};

TConclusion<std::unique_ptr<NTable::IScan>> CreateIScanExportUploader(const TActorId& subscriberActorId,
    const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory,
    const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId, const std::shared_ptr<TDataFileParts>& parts = nullptr);

// With maxDataFileParts the uploader ends the data file before it exceeds this number of multipart upload parts
// and reports the number of rows of the current batch written to the file
std::unique_ptr<IActor> CreateExportUploaderActor(const TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask,
    const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId,
    ui32 initialGeneration = 0, ui32 maxDataFileParts = 0);

}   // namespace NKikimr::NColumnShard::NBackup
