#pragma once
#include <ydb/core/formats/arrow/reader/position.h>
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_context.h>
#include <ydb/core/tx/columnshard/engines/reader/abstract/read_metadata.h>
#include <ydb/core/tx/columnshard/engines/reader/common/stats.h>

#include <ydb/library/formats/arrow/replace_key.h>

namespace NKikimr::NColumnShard {
class TLockSharingInfo;
}

namespace NKikimr::NOlap::NReader::NCommon {
class TSpecialReadContext;
class IDataSource;
class ISourcesConstructor {
private:
    virtual void DoClear() = 0;
    virtual void DoAbort() = 0;
    virtual bool DoIsFinished() const = 0;
    virtual std::shared_ptr<IDataSource> DoExtractNext(const std::shared_ptr<TSpecialReadContext>& context) = 0;
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) = 0;
    virtual TString DoDebugString() const = 0;
    bool InitCursorFlag = false;
    virtual void DoFillReadStats(TReadStats& /*stats*/) const = 0;

public:
    virtual ~ISourcesConstructor() = default;

    void FillReadStats(const std::shared_ptr<TReadStats>& stats) const {
        AFL_VERIFY(stats);
        DoFillReadStats(*stats);
    }

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const {
        return std::vector<TInsertWriteId>();
    }

    TString DebugString() const {
        return DoDebugString();
    }
    void Clear() {
        return DoClear();
    }
    void Abort() {
        return DoAbort();
    }
    bool IsFinished() const {
        return DoIsFinished();
    }
    std::shared_ptr<IDataSource> ExtractNext(const std::shared_ptr<TSpecialReadContext>& context) {
        AFL_VERIFY(!IsFinished());
        AFL_VERIFY(InitCursorFlag);
        auto result = DoExtractNext(context);
        AFL_VERIFY(result);
        return result;
    }
    void InitCursor(const std::shared_ptr<IScanCursor>& cursor) {
        AFL_VERIFY(!InitCursorFlag);
        InitCursorFlag = true;
        if (cursor && cursor->IsInitialized()) {
            return DoInitCursor(cursor);
        }
    }
};

class TReadMetadata: public TReadMetadataBase {
    using TBase = TReadMetadataBase;

private:
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

    virtual TConclusionStatus DoInitCustom(const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription) = 0;

    mutable std::unique_ptr<ISourcesConstructor> SourcesConstructor;

public:
    using TConstPtr = std::shared_ptr<const TReadMetadata>;

    void SetSelectInfo(std::unique_ptr<ISourcesConstructor>&& value) {
        AFL_VERIFY(!SourcesConstructor);
        SourcesConstructor = std::move(value);
    }

    std::unique_ptr<ISourcesConstructor> ExtractSelectInfo() const {
        AFL_VERIFY(!!SourcesConstructor);
        return std::move(SourcesConstructor);
    }

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

    NArrow::NMerger::TSortableBatchPosition BuildSortedPosition(const NArrow::TSimpleRow& key) const;
    virtual std::shared_ptr<IDataReader> BuildReader(const std::shared_ptr<TReadContext>& context) const = 0;

    bool HasProcessingColumnIds() const {
        return GetProgram().HasProcessingColumnIds();
    }

    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    std::shared_ptr<ITableMetadataAccessor> TableMetadataAccessor;
    std::shared_ptr<TReadStats> ReadStats;

    TReadMetadata(const std::shared_ptr<const TVersionedIndex>& schemaIndex, const TReadDescription& read);

    virtual std::vector<TNameTypeInfo> GetKeyYqlSchema() const override {
        return GetResultSchema()->GetIndexInfo().GetPrimaryKeyColumns();
    }

    TConclusionStatus Init(const NColumnShard::TColumnShard* owner, const TReadDescription& readDescription, const bool isPlain);

    std::set<ui32> GetEarlyFilterColumnIds() const;
    std::set<ui32> GetPKColumnIds() const;

    virtual TString DebugString() const override {
        TStringBuilder result;

        result << TBase::DebugString() << " at snapshot: " << GetRequestSnapshot().DebugString();

        if (SourcesConstructor) {
            result << ", " << SourcesConstructor->DebugString();
        }
        return result;
    }
};

}   // namespace NKikimr::NOlap::NReader::NCommon
