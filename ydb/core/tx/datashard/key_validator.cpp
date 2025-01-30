#include "key_validator.h"
#include "datashard_impl.h"
#include "range_ops.h"
#include "datashard_user_db.h"

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>



using namespace NKikimr;
using namespace NKikimr::NDataShard;

TKeyValidator::TKeyValidator(const TDataShard& self) 
    : Self(self)
{

}

void TKeyValidator::AddReadRange(const TTableId& tableId, const TVector<NTable::TColumn>& columns, const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, ui64 itemsLimit, bool reverse)
{
    TVector<TKeyDesc::TColumnOp> columnOps;
    columnOps.reserve(columns.size());
    for (auto& column : columns) {
        TKeyDesc::TColumnOp op;
        op.Column = column.Id;
        op.Operation = TKeyDesc::EColumnOperation::Read;
        op.ExpectedType = column.PType;
        columnOps.emplace_back(std::move(op));
    }

    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::TX_DATASHARD, "-- AddReadRange: " << DebugPrintRange(keyTypes, range, *AppData()->TypeRegistry) << " table: " << tableId);

    auto desc = MakeHolder<TKeyDesc>(tableId, range, TKeyDesc::ERowOperation::Read, keyTypes, columnOps, itemsLimit, 0 /* bytesLimit */, reverse);

    Info.Keys.emplace_back(NMiniKQL::IEngineFlat::TValidatedKey(std::move(desc), /* isWrite */ false));
    ++Info.ReadsCount;
    Info.SetLoaded();
}

void TKeyValidator::AddWriteRange(const TTableId& tableId, const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, const TVector<TColumnWriteMeta>& columns, bool isPureEraseOp)
{
    TVector<TKeyDesc::TColumnOp> columnOps;
    for (const auto& writeColumn : columns) {
        TKeyDesc::TColumnOp op;
        op.Column = writeColumn.Column.Id;
        op.Operation = TKeyDesc::EColumnOperation::Set;
        op.ExpectedType = writeColumn.Column.PType;
        op.ImmediateUpdateSize = writeColumn.MaxValueSizeBytes;
        columnOps.emplace_back(std::move(op));
    }

    LOG_TRACE_S(*NActors::TlsActivationContext, NKikimrServices::TX_DATASHARD, "-- AddWriteRange: " << DebugPrintRange(keyTypes, range, *AppData()->TypeRegistry) << " table: " << tableId);

    auto rowOp = isPureEraseOp ? TKeyDesc::ERowOperation::Erase : TKeyDesc::ERowOperation::Update;
    auto desc = MakeHolder<TKeyDesc>(tableId, range, rowOp, keyTypes, columnOps);

    Info.Keys.emplace_back(NMiniKQL::IEngineFlat::TValidatedKey(std::move(desc), /* isWrite */ true));
    ++Info.WritesCount;
    if (!range.Point) {
        ++Info.DynKeysCount;
    }
    Info.SetLoaded();
}

TKeyValidator::TValidateOptions::TValidateOptions(
        ui64 LockTxId,
        ui32 LockNodeId,
        bool usesMvccSnapshot,
        bool isImmediateTx,
        bool isWriteTx,
        const NTable::TScheme& scheme)
    : IsLockTxId(static_cast<bool>(LockTxId))
    , IsLockNodeId(static_cast<bool>(LockNodeId))
    , UsesMvccSnapshot(usesMvccSnapshot)
    , IsImmediateTx(isImmediateTx)
    , IsWriteTx(isWriteTx)
    , Scheme(scheme)
{
}

bool TKeyValidator::IsValidKey(TKeyDesc& key, const TValidateOptions& opt) const {
    if (TSysTables::IsSystemTable(key.TableId))
        return true;

    if (key.RowOperation != TKeyDesc::ERowOperation::Read) {
        // Prevent updates/erases with LockTxId set, unless it's allowed for immediate mvcc txs
        if (opt.IsLockTxId) {
            if (!(Self.GetEnableLockedWrites() && opt.IsImmediateTx && opt.IsLockNodeId)) {
                key.Status = TKeyDesc::EStatus::OperationNotSupported;
                return false;
            }
        }
        // Prevent updates/erases in mvcc txs
        else if (opt.UsesMvccSnapshot) {
            key.Status = TKeyDesc::EStatus::OperationNotSupported;
            return false;
        }
    }

    ui64 localTableId = Self.GetLocalTableId(key.TableId);
    return NMiniKQL::IsValidKey(opt.Scheme, localTableId, key);
}

ui64 TKeyValidator::GetTableSchemaVersion(const TTableId& tableId) const {
    if (TSysTables::IsSystemTable(tableId))
        return 0;

    const auto& userTables = Self.GetUserTables();
    auto it = userTables.find(tableId.PathId.LocalPathId);
    if (it == userTables.end()) {
        Y_FAIL_S("TKeyValidator (tablet id: " << Self.TabletID() << " state: " << Self.GetState() << ") unable to find given table with id: " << tableId);
        return 0;
    } else {
        return it->second->GetTableSchemaVersion();
    }
}

std::tuple<NMiniKQL::IEngineFlat::EResult, TString> TKeyValidator::ValidateKeys(const TValidateOptions& options) const {
    using EResult = NMiniKQL::IEngineFlat::EResult;

    for (const auto& validKey : Info.Keys) {
        TKeyDesc* key = validKey.Key.get();

        bool valid = IsValidKey(*key, options);

        if (valid) {
            auto curSchemaVersion = GetTableSchemaVersion(key->TableId);
            if (key->TableId.SchemaVersion && curSchemaVersion && curSchemaVersion != key->TableId.SchemaVersion) {
                auto error = TStringBuilder()
                             << "Schema version mismatch for table id: " << key->TableId
                             << " key table version: " << key->TableId.SchemaVersion
                             << " current table version: " << curSchemaVersion;
                return {EResult::SchemeChanged, std::move(error)};
            }
        } else {
            switch (key->Status) {
                case TKeyDesc::EStatus::SnapshotNotExist:
                    return {EResult::SnapshotNotExist, ""};
                case TKeyDesc::EStatus::SnapshotNotReady:
                    key->Status = TKeyDesc::EStatus::Ok;
                    return {EResult::SnapshotNotReady, ""};
                default:
                    auto error = TStringBuilder()
                                 << "Validate (" << __LINE__ << "): Key validation status: " << (ui32)key->Status;
                    return {EResult::KeyError, std::move(error)};
            }
        }
    }

    return {EResult::Ok, ""};
}

NMiniKQL::IEngineFlat::TValidationInfo& TKeyValidator::GetInfo() {
    return Info;
}

const NMiniKQL::IEngineFlat::TValidationInfo& TKeyValidator::GetInfo() const {
    return Info;
}
