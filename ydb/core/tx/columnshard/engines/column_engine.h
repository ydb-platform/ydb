#pragma once
#include "db_wrapper.h"

#include "changes/abstract/compaction_info.h"
#include "changes/abstract/settings.h"
#include "predicate/filter.h"
#include "scheme/snapshot_scheme.h"
#include "scheme/versions/versioned_index.h"

#include <ydb/core/tx/columnshard/common/reverse_accessor.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/counters/common_data.h>
#include <ydb/core/tx/columnshard/resource_subscriber/container.h>
#include <ydb/core/tx/columnshard/tx_reader/abstract.h>

namespace NKikimr::NColumnShard {
class TTiersManager;
}   // namespace NKikimr::NColumnShard

namespace NKikimr::NOlap {
class TInsertColumnEngineChanges;
class TDataAccessorsRequest;
class TCompactColumnEngineChanges;
class TColumnEngineChanges;
class TTTLColumnEngineChanges;
class TCleanupPortionsColumnEngineChanges;
class TCleanupTablesColumnEngineChanges;
class TPortionInfo;
class TDataAccessorsRequest;
namespace NDataLocks {
class TManager;
}

struct TSelectInfo {
    struct TStats {
        size_t Portions{};
        size_t Blobs{};
        size_t Rows{};
        size_t Bytes{};

        const TStats& operator+=(const TStats& stats) {
            Portions += stats.Portions;
            Blobs += stats.Blobs;
            Rows += stats.Rows;
            Bytes += stats.Bytes;
            return *this;
        }
    };

    std::vector<std::shared_ptr<TPortionInfo>> Portions;

    TStats Stats() const;

    TString DebugString() const;
};

class TColumnEngineForLogs;
class IMetadataAccessorResultProcessor {
private:
    virtual void DoApplyResult(NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs& engine) = 0;

public:
    virtual ~IMetadataAccessorResultProcessor() = default;

    void ApplyResult(NResourceBroker::NSubscribe::TResourceContainer<TDataAccessorsResult>&& result, TColumnEngineForLogs& engine) {
        return DoApplyResult(std::move(result), engine);
    }

    IMetadataAccessorResultProcessor() = default;
};

class TCSMetadataRequest {
private:
    YDB_READONLY_DEF(std::shared_ptr<TDataAccessorsRequest>, Request);
    YDB_READONLY_DEF(std::shared_ptr<IMetadataAccessorResultProcessor>, Processor);

public:
    TCSMetadataRequest(const std::shared_ptr<TDataAccessorsRequest>& request, const std::shared_ptr<IMetadataAccessorResultProcessor>& processor)
        : Request(request)
        , Processor(processor) {
        AFL_VERIFY(Request);
        AFL_VERIFY(Processor);
    }
};

class IColumnEngine {
protected:
    virtual void DoRegisterTable(const TInternalPathId pathId) = 0;
    virtual void DoFetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const = 0;

public:
    class TSchemaInitializationData {
    private:
        YDB_READONLY_DEF(std::optional<NKikimrSchemeOp::TColumnTableSchema>, Schema);
        YDB_READONLY_DEF(std::optional<NKikimrSchemeOp::TColumnTableSchemaDiff>, Diff);

    public:
        const NKikimrSchemeOp::TColumnTableSchema& GetSchemaVerified() const {
            AFL_VERIFY(Schema);
            return *Schema;
        }

        TSchemaInitializationData(
            const std::optional<NKikimrSchemeOp::TColumnTableSchema>& schema, const std::optional<NKikimrSchemeOp::TColumnTableSchemaDiff>& diff)
            : Schema(schema)
            , Diff(diff) {
            AFL_VERIFY(Schema || Diff);
        }

        TSchemaInitializationData(const NKikimrTxColumnShard::TSchemaPresetVersionInfo& info) {
            if (info.HasSchema()) {
                Schema = info.GetSchema();
            }
            if (info.HasDiff()) {
                Diff = info.GetDiff();
            }
        }

        ui64 GetVersion() const {
            if (Schema) {
                return Schema->GetVersion();
            }
            AFL_VERIFY(Diff);
            return Diff->GetVersion();
        }
    };

    void FetchDataAccessors(const std::shared_ptr<TDataAccessorsRequest>& request) const;

    static ui64 GetMetadataLimit();

    virtual ~IColumnEngine() = default;

    virtual std::vector<TCSMetadataRequest> CollectMetadataRequests() const = 0;
    virtual const TVersionedIndex& GetVersionedIndex() const = 0;
    virtual const std::shared_ptr<TVersionedIndex>& GetVersionedIndexReadonlyCopy() = 0;
    virtual std::shared_ptr<TVersionedIndex> CopyVersionedIndexPtr() const = 0;
    virtual const std::shared_ptr<arrow::Schema>& GetReplaceKey() const;

    virtual bool HasDataInPathId(const TInternalPathId pathId) const = 0;
    virtual bool ErasePathId(const TInternalPathId pathId) = 0;
    virtual std::shared_ptr<ITxReader> BuildLoader(const std::shared_ptr<IBlobGroupSelector>& dsGroupSelector) = 0;
    void RegisterTable(const TInternalPathId pathId) {
        AFL_DEBUG(NKikimrServices::TX_COLUMNSHARD)("event", "RegisterTable")("path_id", pathId);
        return DoRegisterTable(pathId);
    }
    virtual bool IsOverloadedByMetadata(const ui64 limit) const = 0;
    virtual std::shared_ptr<TSelectInfo> Select(
        TInternalPathId pathId, TSnapshot snapshot, const TPKRangesFilter& pkRangesFilter, const bool withUncommitted) const = 0;
    virtual std::shared_ptr<TInsertColumnEngineChanges> StartInsert(std::vector<TCommittedData>&& dataToIndex) noexcept = 0;
    virtual std::shared_ptr<TColumnEngineChanges> StartCompaction(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept = 0;
    virtual ui64 GetCompactionPriority(const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const std::set<TInternalPathId>& pathIds,
        const std::optional<ui64> waitingPriority) const noexcept = 0;
    virtual std::shared_ptr<TCleanupPortionsColumnEngineChanges> StartCleanupPortions(const TSnapshot& snapshot,
        const THashSet<TInternalPathId>& pathsToDrop, const std::shared_ptr<NDataLocks::TManager>& dataLocksManager) noexcept = 0;
    virtual std::shared_ptr<TCleanupTablesColumnEngineChanges> StartCleanupTables(const THashSet<TInternalPathId>& pathsToDrop) noexcept = 0;
    virtual std::vector<std::shared_ptr<TTTLColumnEngineChanges>> StartTtl(const THashMap<TInternalPathId, TTiering>& pathEviction,
        const std::shared_ptr<NDataLocks::TManager>& dataLocksManager, const ui64 memoryUsageLimit) noexcept = 0;
    virtual bool ApplyChangesOnTxCreate(std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) noexcept = 0;
    virtual bool ApplyChangesOnExecute(IDbWrapper& db, std::shared_ptr<TColumnEngineChanges> changes, const TSnapshot& snapshot) noexcept = 0;
    virtual void RegisterSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, TIndexInfo&& info) = 0;
    virtual void RegisterSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema) = 0;
    virtual void RegisterOldSchemaVersion(const TSnapshot& snapshot, const ui64 presetId, const TSchemaInitializationData& schema) = 0;

    virtual ui64 MemoryUsage() const {
        return 0;
    }
    virtual TSnapshot LastUpdate() const {
        return TSnapshot::Zero();
    }
    virtual void OnTieringModified(const std::optional<NOlap::TTiering>& ttl, const TInternalPathId pathId) = 0;
    virtual void OnTieringModified(const THashMap<TInternalPathId, NOlap::TTiering>& ttl) = 0;
};

}   // namespace NKikimr::NOlap
