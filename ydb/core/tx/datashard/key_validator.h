#pragma once


#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tablet_flat/flat_table_column.h>

namespace NKikimr::NDataShard {

class TKeyValidator {
public:

    struct TColumnWriteMeta {
        NTable::TColumn Column;
        ui32 MaxValueSizeBytes = 0;
    };

    void AddReadRange(const TTableId& tableId, const TVector<NTable::TColumn>& columns, const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, ui64 itemsLimit = 0, bool reverse = false);
    void AddWriteRange(const TTableId& tableId, const TTableRange& range, const TVector<NScheme::TTypeInfo>& keyTypes, const TVector<TColumnWriteMeta>& columns, bool isPureEraseOp);

    NMiniKQL::IEngineFlat::TValidationInfo& GetInfo();
    const NMiniKQL::IEngineFlat::TValidationInfo& GetInfo() const;
private:
    NMiniKQL::IEngineFlat::TValidationInfo Info;
};

}
