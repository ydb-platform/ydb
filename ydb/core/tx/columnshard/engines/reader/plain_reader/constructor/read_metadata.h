#pragma once
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/library/formats/arrow/replace_key.h>
#include <ydb/core/tx/columnshard/engines/reader/common/stats.h>
#include <ydb/core/formats/arrow/reader/position.h>

namespace NKikimr::NColumnShard {
class TLockSharingInfo;
}

namespace NKikimr::NOlap::NReader::NPlain {

// Holds all metadata that is needed to perform read/scan
struct TReadMetadata : public TReadMetadataBase {
    using TBase = TReadMetadataBase;

private:
    const ui64 PathId;
    std::shared_ptr<TAtomicCounter> BrokenWithCommitted = std::make_shared<TAtomicCounter>();
    std::shared_ptr<NColumnShard::TLockSharingInfo> LockSharingInfo;

    class TWriteIdInfo {
    private:
        const ui64 LockId;
        std::shared_ptr<TAtomicCounter> Conflicts;

    public:
        TWriteIdInfo(const ui64 lockId, const std::shared_ptr<TAtomicCounter>& counter)
            : LockId(lockId)
            , Conflicts(counter) {
        }

        ui64 GetLockId() const {
            return LockId;
        }

        void MarkAsConflictable() const {
            Conflicts->Inc();
        }

        bool IsConflictable() const {
            return Conflicts->Val();
        }
    };

    THashMap<ui64, std::shared_ptr<TAtomicCounter>> LockConflictCounters;
    THashMap<TInsertWriteId, TWriteIdInfo> ConflictedWriteIds;

    virtual void DoOnReadFinished(NColumnShard::TColumnShard& owner) const override;
    virtual void DoOnBeforeStartReading(NColumnShard::TColumnShard& owner) const override;
    virtual void DoOnReplyConstruction(const ui64 tabletId, NKqp::NInternalImplementation::TEvScanData& scanData) const override;

public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    bool GetBrokenWithCommitted() const {
        return BrokenWithCommitted->Val();
    }
    THashSet<ui64> GetConflictableLockIds() const {
        THashSet<ui64> result;
        for (auto&& i : ConflictedWriteIds) {
            if (i.second.IsConflictable()) {
                result.emplace(i.second.GetLockId());
            }
        }
        return result;
    }

    bool IsLockConflictable(const ui64 lockId) const {
        auto it = LockConflictCounters.find(lockId);
        AFL_VERIFY(it != LockConflictCounters.end());
        return it->second->Val();
    }

    bool IsWriteConflictable(const TInsertWriteId writeId) const {
        auto it = ConflictedWriteIds.find(writeId);
        AFL_VERIFY(it != ConflictedWriteIds.end());
        return it->second.IsConflictable();
    }

    void AddWriteIdToCheck(const TInsertWriteId writeId, const ui64 lockId) {
        auto it = LockConflictCounters.find(lockId);
        if (it == LockConflictCounters.end()) {
            it = LockConflictCounters.emplace(lockId, std::make_shared<TAtomicCounter>()).first;
        }
        AFL_VERIFY(ConflictedWriteIds.emplace(writeId, TWriteIdInfo(lockId, it->second)).second);
    }

    [[nodiscard]] bool IsMyUncommitted(const TInsertWriteId writeId) const;

    void SetConflictedWriteId(const TInsertWriteId writeId) const {
        auto it = ConflictedWriteIds.find(writeId);
        AFL_VERIFY(it != ConflictedWriteIds.end());
        it->second.MarkAsConflictable();
    }

    void SetBrokenWithCommitted() const {
        BrokenWithCommitted->Inc();
    }

    NArrow::NMerger::TSortableBatchPosition BuildSortedPosition(const NArrow::TReplaceKey& key) const;
    std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<TReadContext>& context) const;

    bool HasProcessingColumnIds() const {
        return GetProgram().HasProcessingColumnIds();
    }

    ui64 GetPathId() const {
        return PathId;
    }

    std::shared_ptr<TSelectInfo> SelectInfo;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    std::vector<TCommittedBlob> CommittedBlobs;
    std::shared_ptr<TReadStats> ReadStats;

    TReadMetadata(const ui64 pathId, const std::shared_ptr<TVersionedIndex> info, const TSnapshot& snapshot, const ESorting sorting, const TProgramContainer& ssaProgram)
        : TBase(info, sorting, ssaProgram, info->GetSchema(snapshot), snapshot)
        , PathId(pathId)
        , ReadStats(std::make_shared<TReadStats>())
    {
    }

    virtual std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return GetResultSchema()->GetIndexInfo().GetPrimaryKeyColumns();
    }

    TConclusionStatus Init(const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription, const TDataStorageAccessor& dataAccessor);

    std::vector<std::string> GetColumnsOrder() const {
        auto schema = GetResultSchema();
        std::vector<std::string> result;
        for (auto&& i : schema->GetSchema()->fields()) {
            result.emplace_back(i->name());
        }
        return result;
    }

    std::set<ui32> GetEarlyFilterColumnIds() const;
    std::set<ui32> GetPKColumnIds() const;

    bool Empty() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->PortionsOrderedPK.empty() && CommittedBlobs.empty();
    }

    size_t NumIndexedChunks() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->NumChunks();
    }

    size_t NumIndexedBlobs() const {
        Y_ABORT_UNLESS(SelectInfo);
        return SelectInfo->Stats().Blobs;
    }

    std::unique_ptr<TScanIteratorBase> StartScan(const std::shared_ptr<TReadContext>& readContext) const override;

    void Dump(IOutputStream& out) const override {
        out << " index chunks: " << NumIndexedChunks()
            << " index blobs: " << NumIndexedBlobs()
            << " committed blobs: " << CommittedBlobs.size()
      //      << " with program steps: " << (Program ? Program->Steps.size() : 0)
            << " at snapshot: " << GetRequestSnapshot().DebugString();
        TBase::Dump(out);
        if (SelectInfo) {
            out << ", ";
            SelectInfo->DebugStream(out);
        }
    }

    friend IOutputStream& operator << (IOutputStream& out, const TReadMetadata& meta) {
        meta.Dump(out);
        return out;
    }
};

}
