#include "clean_orphaned_operations.h"

#include <ydb/core/protos/tx_columnshard.pb.h>
#include <ydb/core/tx/columnshard/blobs_action/blob_manager_db.h>
#include <ydb/core/tx/columnshard/columnshard_schema.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD

namespace NKikimr::NOlap {

namespace {

using namespace NColumnShard;

constexpr size_t OperationsPerTask = 1000;

struct TOrphanedPortion {
    ui64 PortionId = 0;
    std::vector<TUnifiedBlobId> BlobIds;
    std::vector<std::pair<ui32, ui32>> IndexChunks;
};

struct TOrphanedOperation {
    ui64 WriteId = 0;
    ui64 PathId = 0;
    THashSet<ui64> InsertWriteIds;
    std::vector<TOrphanedPortion> Portions;
};

}   // namespace

class TCleanOrphanedOperationsNormalizer::TChanges: public INormalizerChanges {
private:
    const ui64 TabletId;
    const std::vector<TOrphanedOperation> Operations;

public:
    TChanges(const ui64 tabletId, std::vector<TOrphanedOperation>&& operations)
        : TabletId(tabletId)
        , Operations(std::move(operations))
    {
    }

    bool ApplyOnExecute(NTabletFlatExecutor::TTransactionContext& txc, const TNormalizationController& normController) const override {
        NIceDb::TNiceDb db(txc.DB);
        TBlobManagerDb blobManagerDb(txc.DB);
        for (const auto& operation : Operations) {
            for (const auto& portion : operation.Portions) {
                for (const auto& blobId : portion.BlobIds) {
                    blobManagerDb.AddBlobToDelete(blobId, (TTabletId)TabletId);
                }
                for (const auto& [indexId, chunkIdx] : portion.IndexChunks) {
                    db.Table<Schema::IndexIndexes>().Key(operation.PathId, portion.PortionId, indexId, chunkIdx).Delete();
                }
                db.Table<Schema::IndexColumnsV2>().Key(operation.PathId, portion.PortionId).Delete();
                db.Table<Schema::IndexPortions>().Key(operation.PathId, portion.PortionId).Delete();
            }
            db.Table<Schema::Operations>().Key(operation.WriteId).Delete();
        }
        normController.AddNormalizerEvent(db, "CLEAN_ORPHANED_OPERATIONS", DebugString());
        return true;
    }

    ui64 GetSize() const override {
        return Operations.size();
    }

    TString DebugString() const override {
        ui64 portions = 0;
        ui64 blobs = 0;
        for (const auto& operation : Operations) {
            portions += operation.Portions.size();
            for (const auto& portion : operation.Portions) {
                blobs += portion.BlobIds.size();
            }
        }
        return TStringBuilder() << "operations=" << Operations.size() << ";portions=" << portions << ";blobs=" << blobs;
    }
};

TConclusion<std::vector<INormalizerTask::TPtr>> TCleanOrphanedOperationsNormalizer::DoInit(
    const TNormalizationController& /*controller*/, NTabletFlatExecutor::TTransactionContext& txc) {
    NIceDb::TNiceDb db(txc.DB);

    THashSet<ui64> knownPathIds;
    {
        auto rowset = db.Table<Schema::TableInfo>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("cannot read TableInfo");
        }
        while (!rowset.EndOfSet()) {
            knownPathIds.insert(rowset.GetValue<Schema::TableInfo::PathId>());
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("cannot read TableInfo");
            }
        }
    }

    THashSet<ui64> proposedLockIds;
    {
        auto rowset = db.Table<Schema::OperationTxIds>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("cannot read OperationTxIds");
        }
        while (!rowset.EndOfSet()) {
            proposedLockIds.insert(rowset.GetValue<Schema::OperationTxIds::LockId>());
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("cannot read OperationTxIds");
            }
        }
    }

    std::vector<TOrphanedOperation> operations;
    {
        auto rowset = db.Table<Schema::Operations>().Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("cannot read Operations");
        }
        while (!rowset.EndOfSet()) {
            NKikimrTxColumnShard::TInternalOperationData metaProto;
            AFL_VERIFY(metaProto.ParseFromString(rowset.GetValue<Schema::Operations::Metadata>()));
            const ui64 pathId = TInternalPathId::FromProto(metaProto).GetRawValue();
            if (!knownPathIds.contains(pathId) || !proposedLockIds.contains(rowset.GetValue<Schema::Operations::LockId>())) {
                TOrphanedOperation operation{ .WriteId = rowset.GetValue<Schema::Operations::WriteId>(), .PathId = pathId };
                for (const ui64 insertWriteId : metaProto.GetInternalWriteIds()) {
                    operation.InsertWriteIds.insert(insertWriteId);
                }
                operations.push_back(std::move(operation));
            }
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("cannot read Operations");
            }
        }
    }
    if (operations.empty()) {
        return std::vector<INormalizerTask::TPtr>();
    }

    THashMap<ui64, THashMap<ui64, TOrphanedOperation*>> operationsByPathAndInsertWriteId;
    for (auto& operation : operations) {
        for (const ui64 insertWriteId : operation.InsertWriteIds) {
            operationsByPathAndInsertWriteId[operation.PathId][insertWriteId] = &operation;
        }
    }
    for (const auto& [pathId, operationsByInsertWriteId] : operationsByPathAndInsertWriteId) {
        auto rowset = db.Table<Schema::IndexPortions>().Prefix(pathId).Select();
        if (!rowset.IsReady()) {
            return TConclusionStatus::Fail("cannot read IndexPortions");
        }
        while (!rowset.EndOfSet()) {
            const auto it = operationsByInsertWriteId.find(rowset.GetValueOrDefault<Schema::IndexPortions::InsertWriteId>(0));
            if (it != operationsByInsertWriteId.end()) {
                it->second->Portions.push_back({ .PortionId = rowset.GetValue<Schema::IndexPortions::PortionId>() });
            }
            if (!rowset.Next()) {
                return TConclusionStatus::Fail("cannot read IndexPortions");
            }
        }
    }
    for (auto& operation : operations) {
        for (auto& portion : operation.Portions) {
            {
                auto rowset = db.Table<Schema::IndexColumnsV2>().Key(operation.PathId, portion.PortionId).Select();
                if (!rowset.IsReady()) {
                    return TConclusionStatus::Fail("cannot read IndexColumnsV2");
                }
                if (!rowset.EndOfSet()) {
                    portion.BlobIds = TColumnChunkLoadContextV2(rowset, DsGroupSelector).GetBlobIds();
                }
            }
            {
                auto rowset = db.Table<Schema::IndexIndexes>().Prefix(operation.PathId, portion.PortionId).Select();
                if (!rowset.IsReady()) {
                    return TConclusionStatus::Fail("cannot read IndexIndexes");
                }
                while (!rowset.EndOfSet()) {
                    portion.IndexChunks.emplace_back(
                        rowset.GetValue<Schema::IndexIndexes::IndexId>(), rowset.GetValue<Schema::IndexIndexes::ChunkIdx>());
                    if (!rowset.Next()) {
                        return TConclusionStatus::Fail("cannot read IndexIndexes");
                    }
                }
            }
        }
    }

    YDB_LOG_WARN("",
        {"normalizer", GetClassName()},
        {"orphaned_operations", operations.size()});
    std::vector<INormalizerTask::TPtr> tasks;
    for (size_t from = 0; from < operations.size(); from += OperationsPerTask) {
        const size_t to = std::min(operations.size(), from + OperationsPerTask);
        std::vector<TOrphanedOperation> batch(
            std::make_move_iterator(operations.begin() + from), std::make_move_iterator(operations.begin() + to));
        tasks.push_back(std::make_shared<TTrivialNormalizerTask>(std::make_shared<TChanges>(TabletId, std::move(batch))));
    }
    return tasks;
}

}   // namespace NKikimr::NOlap
