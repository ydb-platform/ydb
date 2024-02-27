#pragma once

#include <ydb/core/tablet_flat/flat_database.h>

#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tablet_flat/flat_table_column.h>

namespace NKikimr::NDataShard {

class TDataShard;
class TDataShardUserDb;

class TKeyValidator {
public:
    TKeyValidator(const TDataShard& self, const NTable::TDatabase& db);

    struct TColumnWriteMeta {
        NTable::TColumn Column;
        ui32 MaxValueSizeBytes = 0;
    };

    void AddReadRange(const TTableId& tableId, const TVector<NTable::TColumn>& columns, const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, ui64 itemsLimit = 0, bool reverse = false);
    void AddWriteRange(const TTableId& tableId, const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, const TVector<TColumnWriteMeta>& columns, bool isPureEraseOp);
    
    struct TValidateOptions {
        bool IsLockTxId;
        bool IsLockNodeId;
        bool IsRepeatableSnapshot;
        bool IsImmediateTx;
        bool IsWriteTx;

        TValidateOptions(ui64 LockTxId,
                         ui32 LockNodeId,
                         bool isRepeatableSnapshot,
                         bool isImmediateTx,
                         bool isWriteTx);
    };

    bool IsValidKey(TKeyDesc& key, const TValidateOptions& options) const;
    std::tuple<NMiniKQL::IEngineFlat::EResult, TString> ValidateKeys(const TValidateOptions& options) const;

    ui64 GetTableSchemaVersion(const TTableId& tableId) const;

    NMiniKQL::IEngineFlat::TValidationInfo& GetInfo();
    const NMiniKQL::IEngineFlat::TValidationInfo& GetInfo() const;
private:
    const TDataShard& Self;
    const NTable::TDatabase& Db;

    NMiniKQL::IEngineFlat::TValidationInfo Info;
};

}
