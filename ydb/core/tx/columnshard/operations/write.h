#pragma once

#include <ydb/core/tx/ev_write/write_data.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/defs.h>
#include <ydb/core/protos/tx_columnshard.pb.h>

#include <ydb/core/tablet_flat/flat_cxx_database.h>
#include <ydb/library/accessor/accessor.h>

#include <util/generic/map.h>
#include <tuple>


namespace NKikimr::NTabletFlatExecutor {
    class TTransactionContext;
}

namespace NKikimr::NColumnShard {

    class TColumnShard;

    using TWriteId = NOlap::TWriteId;

    enum class EOperationStatus : ui32 {
        Draft = 1,
        Started = 2,
        Prepared = 3
    };

    class TWriteOperation {
        YDB_READONLY(EOperationStatus, Status, EOperationStatus::Draft);
        YDB_READONLY_DEF(TInstant, CreatedAt);
        YDB_READONLY_DEF(TWriteId, WriteId);
        YDB_READONLY(ui64, TxId, 0);
        YDB_READONLY_DEF(TVector<TWriteId>, GlobalWriteIds);

    public:
        using TPtr = std::shared_ptr<TWriteOperation>;

        TWriteOperation(const TWriteId writeId, const ui64 txId, const EOperationStatus& status, const TInstant createdAt);

        void Start(TColumnShard& owner, const ui64 tableId, const NEvWrite::IDataContainer::TPtr& data, const NActors::TActorId& source, const TActorContext& ctx);
        void OnWriteFinish(NTabletFlatExecutor::TTransactionContext& txc, const TVector<TWriteId>& globalWriteIds);
        void Commit(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot) const;
        void Abort(TColumnShard& owner, NTabletFlatExecutor::TTransactionContext& txc) const;

        void Out(IOutputStream& out) const {
            out << "write_id=" << (ui64) WriteId << ";tx_id=" << TxId;
        }

        void ToProto(NKikimrTxColumnShard::TInternalOperationData& proto) const;
        void FromProto(const NKikimrTxColumnShard::TInternalOperationData& proto);
    };

    class TOperationsManager {
        TMap<ui64, TVector<TWriteId>> Transactions;
        TMap<TWriteId, TWriteOperation::TPtr> Operations;
        TWriteId LastWriteId = TWriteId(0);

    public:
        bool Init(NTabletFlatExecutor::TTransactionContext& txc);

        TWriteOperation::TPtr GetOperation(const TWriteId writeId) const;
        bool CommitTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc, const NOlap::TSnapshot& snapshot);
        bool AbortTransaction(TColumnShard& owner, const ui64 txId, NTabletFlatExecutor::TTransactionContext& txc);

        TWriteOperation::TPtr RegisterOperation(const ui64 txId);
    private:
        TWriteId BuildNextWriteId();
        void RemoveOperation(const TWriteOperation::TPtr& op, NTabletFlatExecutor::TTransactionContext& txc);
    };
}

template <>
inline void Out<NKikimr::NColumnShard::TWriteOperation>(IOutputStream& o, const NKikimr::NColumnShard::TWriteOperation& x) {
    return x.Out(o);
}
