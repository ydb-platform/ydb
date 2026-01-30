#pragma once

#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <ydb/core/formats/arrow/converter.h>
#include <ydb/core/tx/datashard/export_iface.h>
#include <ydb/library/conclusion/result.h>


namespace NKikimr::NColumnShard::NBackup {

class TRowWriter : public NArrow::IRowWriter {
public:
    explicit TRowWriter(TVector<TSerializedCellVec>& rows);
    void AddRow(const TConstArrayRef<TCell>& cells) override;
private:
    TVector<TSerializedCellVec>& Rows;
};

struct TExportDriver final : NTable::IDriver {
    TExportDriver(TActorSystem* actorSystem, const TActorId& subscriberActorId);

    virtual void Touch(NTable::EScan state) override;

    virtual void Throw(const std::exception& e) override;

    virtual ui64 GetTotalCpuTimeUs() const override;

private:
    NActors::TActorSystem* ActorSystem;
    TActorId SubscriberActorId;
};

TConclusion<TVector<TSerializedCellVec>> BatchToRows(const std::shared_ptr<arrow::RecordBatch>& batch, const TVector<std::pair<TString, NScheme::TTypeInfo>>& ydbSchema);

TConclusion<std::unique_ptr<NTable::IScan>> CreateIScanExportUploader(const TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId);

std::unique_ptr<IActor> CreateExportUploaderActor(const TActorId& subscriberActorId, const NKikimrSchemeOp::TBackupTask& backupTask, const NDataShard::IExportFactory* exportFactory, const NDataShard::IExport::TTableColumns& tableColumns, ui64 txId);

} // namespace NKikimr::NColumnShard::NBackup