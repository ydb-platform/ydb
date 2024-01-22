#include "key_validator.h"
#include "ydb/core/base/appdata_fwd.h"


#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/tx/datashard/range_ops.h>

using namespace NKikimr;
using namespace NKikimr::NDataShard;

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

NMiniKQL::IEngineFlat::TValidationInfo& TKeyValidator::GetInfo() {
    return Info;
}

const NMiniKQL::IEngineFlat::TValidationInfo& TKeyValidator::GetInfo() const {
    return Info;
}
