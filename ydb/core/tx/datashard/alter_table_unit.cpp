#include "datashard_impl.h"
#include "datashard_locks_db.h"
#include "datashard_pipeline.h"
#include "execution_unit_ctors.h"

#include <ydb/library/aclib/user_context.h>

#include <utility>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_DATASHARD

namespace NKikimr {
namespace NDataShard {

namespace {

using TFamilyKey = std::pair<TString, ui32>;

TFamilyKey FamilyKey(const TUserTable& table, ui32 id) {
    if (const auto it = table.Families.find(id); it != table.Families.end()) {
        const auto name = it->second.GetName();
        // GetName() returns "default" for every unnamed family.
        return name == "default" && id != 0 ? TFamilyKey{"", id} : TFamilyKey{name, 0};
    }

    Y_ENSURE(id == 0, "Unknown column family: " << id);
    return {"default", 0};
}

struct TFamilySettings {
    NTable::NPage::ECodec Codec = NTable::NPage::ECodec::Plain;
    NTable::NPage::ECacheMode CacheMode = NTable::NPage::ECacheMode::Regular;
    TString DataPoolKind;

    bool operator==(const TFamilySettings&) const = default;
};

TMap<TFamilyKey, TFamilySettings> FamilySettings(const TUserTable& table) {
    TMap<TFamilyKey, TFamilySettings> result;
    result.emplace(TFamilyKey{"default", 0}, TFamilySettings{});

    for (const auto& [id, family] : table.Families) {
        const auto key = FamilyKey(table, id);
        const TFamilySettings settings{
            .Codec = family.Codec,
            .CacheMode = family.CacheMode,
            .DataPoolKind = family.StorageConfig.GetData().GetPreferredPoolKind(),
        };

        if (key == TFamilyKey{"default", 0}) {
            result[key] = settings;
        } else {
            Y_ENSURE(result.emplace(key, settings).second, "Duplicate column family: " << key.first);
        }
    }

    return result;
}

bool FamilySchemaChanged(const TUserTable& oldTable, const TUserTable& newTable) {
    if (FamilySettings(oldTable) != FamilySettings(newTable)) {
        return true;
    }

    for (const auto& [id, column] : newTable.Columns) {
        auto it = oldTable.Columns.find(id);
        if (it == oldTable.Columns.end()) {
            continue;
        }

        if (FamilyKey(oldTable, it->second.Family) != FamilyKey(newTable, column.Family)) {
            return true;
        }
    }

    return false;
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

class TAlterMoveShadowUnit : public TExecutionUnit {
public:
    TAlterMoveShadowUnit(TDataShard& dataShard, TPipeline& pipeline)
        : TExecutionUnit(EExecutionUnitKind::AlterMoveShadow, false, dataShard, pipeline)
    { }

    bool IsReadyToExecute(TOperation::TPtr op) const override {
        if (!op->IsWaitingForSnapshot())
            return true;

        return !op->InputSnapshots().empty();
    }

    EExecutionStatus Execute(TOperation::TPtr op, TTransactionContext& txc, const TActorContext& ctx) override {
        Y_UNUSED(ctx);

        TActiveTransaction* tx = dynamic_cast<TActiveTransaction*>(op.Get());
        Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

        // AlterMoveShadow is only applicable when AlterTable or PrepareIndexValidation is in the transaction
        auto& schemeTx = tx->GetSchemeTx();
        bool shadowDisabled = false;
        ui64 tableId = 0;
        if (schemeTx.HasAlterTable()) {
            // Only applicable when ALTER TABLE has disabled ShadowData
            const auto& alter = schemeTx.GetAlterTable();
            shadowDisabled = (
                alter.HasPartitionConfig() &&
                alter.GetPartitionConfig().HasShadowData() &&
                !alter.GetPartitionConfig().GetShadowData());
            tableId = alter.GetId_Deprecated();
            if (alter.HasPathId()) {
                auto& pathId = alter.GetPathId();
                Y_ENSURE(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
                tableId = pathId.GetLocalId();
            }
        } else if (schemeTx.HasPrepareIndexValidation()) {
            const auto& snap = schemeTx.GetPrepareIndexValidation();
            shadowDisabled = true;
            tableId = snap.GetIndexId().GetLocalId();
            Y_ENSURE(DataShard.GetPathOwnerId() == snap.GetIndexId().GetOwnerId());
        } else {
            return EExecutionStatus::Executed;
        }

        // Only applicable when tx has disabled ShadowData
        if (!shadowDisabled)
            return EExecutionStatus::Executed;

        // Only applicable when table has ShadowData enabled
        Y_ENSURE(DataShard.GetUserTables().contains(tableId));
        const TUserTable& table = *DataShard.GetUserTables().at(tableId);
        const ui32 localTid = table.LocalTid;
        const ui32 shadowTid = table.ShadowTid;
        if (!shadowTid)
            return EExecutionStatus::Executed;

        // We must create shadow table snapshot
        if (!op->IsWaitingForSnapshot()) {
            TIntrusivePtr<TTableSnapshotContext> snapContext
                = new TTxTableSnapshotContext(op->GetStep(), op->GetTxId(), { shadowTid });
            txc.Env.MakeSnapshot(snapContext);

            op->SetWaitingForSnapshotFlag();
            return EExecutionStatus::Continue;
        }

        Y_ENSURE(op->InputSnapshots().size() == 1, "Expected a single shadow snapshot");
        {
            auto& snapshot = op->InputSnapshots()[0];
            txc.Env.MoveSnapshot(*snapshot, /* src */ shadowTid, /* dst */ localTid);
            txc.Env.DropSnapshot(snapshot);
        }

        // Snapshot move cannot be mixed with other operations on shadowTid
        // We have to wait for completion before dropping shadow table
        op->InputSnapshots().clear();
        op->ResetWaitingForSnapshotFlag();
        return EExecutionStatus::WaitComplete;
    }

    void Complete(TOperation::TPtr op, const TActorContext& ctx) override {
        Y_UNUSED(op);
        Y_UNUSED(ctx);
    }
};

THolder<TExecutionUnit> CreateAlterMoveShadowUnit(TDataShard& dataShard, TPipeline& pipeline)
{
    return THolder(new TAlterMoveShadowUnit(dataShard, pipeline));
}

////////////////////////////////////////////////////////////////////////////////

class TAlterTableUnit : public TExecutionUnit {
public:
    TAlterTableUnit(TDataShard &dataShard,
                    TPipeline &pipeline);
    ~TAlterTableUnit() override;

    bool IsReadyToExecute(TOperation::TPtr op) const override;
    EExecutionStatus Execute(TOperation::TPtr op,
                             TTransactionContext &txc,
                             const TActorContext &ctx) override;
    void Complete(TOperation::TPtr op,
                  const TActorContext &ctx) override;

private:
};

TAlterTableUnit::TAlterTableUnit(TDataShard &dataShard,
                                 TPipeline &pipeline)
    : TExecutionUnit(EExecutionUnitKind::AlterTable, false, dataShard, pipeline)
{
}

TAlterTableUnit::~TAlterTableUnit()
{
}

bool TAlterTableUnit::IsReadyToExecute(TOperation::TPtr) const
{
    return true;
}

EExecutionStatus TAlterTableUnit::Execute(TOperation::TPtr op,
                                          TTransactionContext &txc,
                                          const TActorContext &ctx)
{
    TActiveTransaction *tx = dynamic_cast<TActiveTransaction*>(op.Get());
    Y_ENSURE(tx, "cannot cast operation of kind " << op->GetKind());

    auto &schemeTx = tx->GetSchemeTx();
    if (!schemeTx.HasAlterTable())
        return EExecutionStatus::Executed;

    const auto& alterTableTx = schemeTx.GetAlterTable();

    const auto version = alterTableTx.GetTableSchemaVersion();
    Y_ENSURE(version);

    YDB_LOG_INFO_CTX(ctx, "TAlterTableUnit::Execute: trying to alter table",
        {"tabletId", DataShard.TabletID()},
        {"version", version});

    TPathId tableId(DataShard.GetPathOwnerId(), alterTableTx.GetId_Deprecated());
    if (alterTableTx.HasPathId()) {
        auto& pathId = alterTableTx.GetPathId();
        Y_ENSURE(DataShard.GetPathOwnerId() == pathId.GetOwnerId());
        tableId.LocalPathId = pathId.GetLocalId();
    }

    auto oldInfo = DataShard.FindUserTable(tableId);
    auto newInfo = DataShard.AlterUserTable(ctx, txc, alterTableTx);
    TDataShardLocksDb locksDb(DataShard, txc);
    DataShard.ReplaceUserTable(tableId, newInfo, locksDb);

    if (newInfo->NeedSchemaSnapshots()) {
        DataShard.AddSchemaSnapshot(tableId, version, op->GetStep(), op->GetTxId(), txc, ctx);
    }

    bool schemaChanged = FamilySchemaChanged(*oldInfo, *newInfo);
    if (alterTableTx.DropColumnsSize()) {
        schemaChanged = true;
    } else {
        for (const auto& [tag, column] : newInfo->Columns) {
            if (!oldInfo->Columns.contains(tag)) {
                schemaChanged = true;
                break;
            }
        }
    }

    if (schemaChanged) {
        NIceDb::TNiceDb db(txc.DB);

        for (const auto& streamPathId : newInfo->GetSchemaChangesCdcStreams()) {
            auto recordPtr = TChangeRecordBuilder(TChangeRecord::EKind::CdcSchemaChange)
                .WithOrder(DataShard.AllocateChangeRecordOrder(db))
                .WithGroup(0)
                .WithStep(op->GetStep())
                .WithTxId(op->GetTxId())
                .WithPathId(streamPathId)
                .WithTableId(tableId)
                .WithSchemaVersion(newInfo->GetTableSchemaVersion())
                .WithUserCtx(NACLib::TUserContextBuilder().WithUserSID(BUILTIN_ACL_CDC_WITHOUT_USER_SID).Build())
                .Build();

            const auto& record = *recordPtr;
            DataShard.PersistChangeRecord(db, record);

            op->ChangeRecords().push_back(IDataShardChangeCollector::TChange{
                .Order = record.GetOrder(),
                .Group = record.GetGroup(),
                .Step = record.GetStep(),
                .TxId = record.GetTxId(),
                .PathId = record.GetPathId(),
                .BodySize = 0,
                .TableId = record.GetTableId(),
                .SchemaVersion = record.GetSchemaVersion(),
            });
        }
    }

    BuildResult(op, NKikimrTxDataShard::TEvProposeTransactionResult::COMPLETE);
    op->Result()->SetStepOrderId(op->GetStepOrder().ToPair());

    return EExecutionStatus::DelayCompleteNoMoreRestarts;
}

void TAlterTableUnit::Complete(TOperation::TPtr op,
                               const TActorContext &)
{
    DataShard.EnqueueChangeRecords(std::move(op->ChangeRecords()));
}

THolder<TExecutionUnit> CreateAlterTableUnit(TDataShard &dataShard,
                                             TPipeline &pipeline)
{
    return THolder(new TAlterTableUnit(dataShard, pipeline));
}

} // namespace NDataShard
} // namespace NKikimr


#undef YDB_LOG_THIS_FILE_COMPONENT
