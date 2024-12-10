#pragma once

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/abstract_scheme.h>
#include <ydb/core/tx/data_events/events.h>
#include <ydb/core/tx/data_events/write_data.h>

#include <ydb/library/accessor/accessor.h>

#include <util/generic/map.h>

#include <tuple>

namespace NKikimr::NTabletFlatExecutor {
class TTransactionContext;
}

namespace NKikimr::NOlap::NTxInteractions {
class TManager;
}

namespace NKikimr::NColumnShard {

class TColumnShard;

using TOperationWriteId = NOlap::TOperationWriteId;
using TInsertWriteId = NOlap::TInsertWriteId;

enum class EOperationStatus : ui32 {
    Draft = 1,
    Started = 2,
    Prepared = 3
};

enum class EOperationBehaviour : ui32 {
    Undefined = 1,
    InTxWrite = 2,
    WriteWithLock = 3,
    CommitWriteLock = 4,
    AbortWriteLock = 5,
    NoTxWrite = 6
};

class TWriteOperation {
    YDB_READONLY(EOperationStatus, Status, EOperationStatus::Draft);
    YDB_READONLY_DEF(TInstant, CreatedAt);
    YDB_READONLY_DEF(TOperationWriteId, WriteId);
    YDB_READONLY(ui64, LockId, 0);
    YDB_READONLY(ui64, Cookie, 0);
    YDB_READONLY_DEF(std::vector<TInsertWriteId>, InsertWriteIds);
    YDB_ACCESSOR(EOperationBehaviour, Behaviour, EOperationBehaviour::Undefined);
    YDB_READONLY_DEF(std::optional<ui32>, GranuleShardingVersionId);
    YDB_READONLY(NEvWrite::EModificationType, ModificationType, NEvWrite::EModificationType::Upsert);

public:
    using TPtr = std::shared_ptr<TWriteOperation>;

    TWriteOperation(const TOperationWriteId writeId, const ui64 lockId, const ui64 cookie, const EOperationStatus& status, const TInstant createdAt,
        const std::optional<ui32> granuleShardingVersionId, const NEvWrite::EModificationType mType);

    void Start(TColumnShard& owner, const ui64 tableId, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source,
        const std::shared_ptr<NOlap::ISnapshotSchema>& schema, const TActorContext& ctx, const NOlap::TSnapshot& applyToSnapshot);
    void OnWriteFinish(NTabletFlatExecutor::TTransactionContext& txc, const std::vector<TInsertWriteId>& insertWriteIds, const bool ephemeralFlag);
    void CommitOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const;
    void CommitOnComplete(TColumnShard& owner, const NOlap::TSnapshot& snapshot) const;
    void AbortOnExecute(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const;
    void AbortOnComplete(TColumnShard& owner) const;

    void Out(IOutputStream& out) const {
        out << "write_id=" << (ui64)WriteId << ";lock_id=" << LockId;
    }

    void ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const;
    void FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto);
};

}   // namespace NKikimr::NColumnShard

template <>
inline void Out<NKikimr::NColumnShard::TWriteOperation>(IOutputStream& o, const NKikimr::NColumnShard::TWriteOperation& x) {
    return x.Out(o);
}
