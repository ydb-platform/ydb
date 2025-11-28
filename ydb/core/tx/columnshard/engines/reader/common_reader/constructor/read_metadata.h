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
    virtual std::shared_ptr<IDataSource> DoTryExtractNext(const std::shared_ptr<TSpecialReadContext>& context, const ui32 inFlightCurrentLimit) = 0;
    virtual void DoInitCursor(const std::shared_ptr<IScanCursor>& cursor) = 0;
    virtual TString DoDebugString() const = 0;
    bool InitCursorFlag = false;
    virtual void DoFillReadStats(TReadStats& /*stats*/) const {

    }

public:
    virtual TString GetClassName() const {
        return "UNDEFINED";
    }

    virtual ~ISourcesConstructor() = default;

    void FillReadStats(const std::shared_ptr<TReadStats>& stats) const {
        AFL_VERIFY(stats);
        DoFillReadStats(*stats);
    }

    virtual std::vector<TInsertWriteId> GetUncommittedWriteIds() const {
        return std::vector<TInsertWriteId>();
    }

    TString DebugString() const {
        TStringBuilder sb;
        sb << "{";
        sb << "class_name=" << GetClassName() << ";";
        sb << "internal={" << DoDebugString() << "};";
        sb << "}";
        return sb;
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
    std::shared_ptr<IDataSource> TryExtractNext(const std::shared_ptr<TSpecialReadContext>& context, const ui32 inFlightCurrentLimit) {
        AFL_VERIFY(!IsFinished());
        AFL_VERIFY(InitCursorFlag);
        auto result = DoTryExtractNext(context, inFlightCurrentLimit);
//        AFL_VERIFY(result);
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
    mutable TAtomicCounter BreakLockOnReadFinished = TAtomicCounter();
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

        void MarkAsConflicting() const {
            Conflicts->Inc();
        }

        bool IsConflicting() const {
            return Conflicts->Val();
        }
    };

    THashMap<ui64, std::shared_ptr<TAtomicCounter>> LockConflictCounters;
    THashMap<TInsertWriteId, TWriteIdInfo> ConflictingWrites;

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

    bool GetBreakLockOnReadFinished() const {
        return BreakLockOnReadFinished.Val();
    }

    void SetBreakLockOnReadFinished() const {
        BreakLockOnReadFinished.Inc();
    }

    THashSet<ui64> GetConflictingLockIds() const {
        THashSet<ui64> result;
        for (auto& [_, writeIdInfo] : ConflictingWrites) {
            if (writeIdInfo.IsConflicting()) {
                result.emplace(writeIdInfo.GetLockId());
            }
        }
        return result;
    }

    THashSet<ui64> GetMaybeConflictingLockIds() const {
        THashSet<ui64> result;
        for (auto& [_, writeInfo] : ConflictingWrites) {
            result.emplace(writeInfo.GetLockId());
        }
        return result;
    }

    bool MayWriteBeConflicting(const TInsertWriteId writeId) const {
        return ConflictingWrites.contains(writeId);
    }

    void AddMaybeConflictingWrite(const TInsertWriteId writeId, const ui64 lockId) {
        auto it = LockConflictCounters.find(lockId);
        if (it == LockConflictCounters.end()) {
            it = LockConflictCounters.emplace(lockId, std::make_shared<TAtomicCounter>()).first;
        }
        AFL_VERIFY(ConflictingWrites.emplace(writeId, TWriteIdInfo(lockId, it->second)).second);
    }

    void SetWriteConflicting(const TInsertWriteId writeId) const {
        auto it = ConflictingWrites.find(writeId);
        AFL_VERIFY(it != ConflictingWrites.end());
        it->second.MarkAsConflicting();
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

    TReadMetadata(const TReadMetadata&) = delete;
    TReadMetadata& operator=(const TReadMetadata&) = delete;

    bool OrderByLimitAllowed() const {
        return TableMetadataAccessor->OrderByLimitAllowed() && !GetFakeSort();
    }

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
