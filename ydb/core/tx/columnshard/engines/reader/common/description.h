#pragma once
#include <ydb/core/tx/columnshard/common/path_id.h>
#include <ydb/core/tx/columnshard/common/snapshot.h>
#include <ydb/core/tx/columnshard/engines/metadata_accessor.h>
#include <ydb/core/tx/columnshard/engines/predicate/filter.h>
#include <ydb/core/tx/columnshard/engines/reader/common/scan_memory_limiter.h>
#include <ydb/core/tx/columnshard/operations/manager.h>
#include <ydb/core/tx/program/program.h>

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

namespace NLWTrace {
class TOrbit;
}

namespace NKikimr::NOlap::NReader {

enum class ERequestSorting {
    NONE = 0,
    ASC,
    DESC,
};

enum class ESourcesSorting {
    // no ORDER BY, no deduplication: the order is free, so use the source's own id -- portion id for a
    // table, (tablet, path/schema id) for a sys view
    SourceIdAsc = 0,
    // ORDER BY pk ASC: reading up, a source starts mattering at its first key
    FirstPkAsc,
    // no ORDER BY, deduplication on: ordering by where sources end keeps the duplicates filter window narrow
    LastPkAsc,
    // ORDER BY pk DESC: reading down, a source starts mattering at its last key
    LastPkDesc,
};

inline NKikimrKqp::TEvKqpScanCursor::ESourcesSorting SourcesSortingToProto(const ESourcesSorting sorting) {
    switch (sorting) {
        case ESourcesSorting::SourceIdAsc:
            return NKikimrKqp::TEvKqpScanCursor::SOURCE_ID_ASC;
        case ESourcesSorting::FirstPkAsc:
            return NKikimrKqp::TEvKqpScanCursor::FIRST_PK_ASC;
        case ESourcesSorting::LastPkAsc:
            return NKikimrKqp::TEvKqpScanCursor::LAST_PK_ASC;
        case ESourcesSorting::LastPkDesc:
            return NKikimrKqp::TEvKqpScanCursor::LAST_PK_DESC;
    }
    AFL_VERIFY(false)("sources_sorting", (ui64)sorting);
    return NKikimrKqp::TEvKqpScanCursor::SOURCE_ID_ASC;
}

// Nothing when the peer sent a value this build does not know.
inline std::optional<ESourcesSorting> SourcesSortingFromProto(const NKikimrKqp::TEvKqpScanCursor::ESourcesSorting sorting) {
    switch (sorting) {
        case NKikimrKqp::TEvKqpScanCursor::SOURCE_ID_ASC:
            return ESourcesSorting::SourceIdAsc;
        case NKikimrKqp::TEvKqpScanCursor::FIRST_PK_ASC:
            return ESourcesSorting::FirstPkAsc;
        case NKikimrKqp::TEvKqpScanCursor::LAST_PK_ASC:
            return ESourcesSorting::LastPkAsc;
        case NKikimrKqp::TEvKqpScanCursor::LAST_PK_DESC:
            return ESourcesSorting::LastPkDesc;
    }
    return std::nullopt;
}

// Describes read/scan request
class TReadDescription {
private:
    TSnapshot Snapshot;
    TProgramContainer Program;
    std::optional<std::shared_ptr<IScanCursor>> ScanCursor;
    YDB_ACCESSOR_DEF(TString, ScanIdentifier);
    YDB_READONLY(bool, DeduplicationEnabled, false);
    EReaderClass ReaderClass = EReaderClass::Trivial;
    YDB_READONLY_DEF(std::shared_ptr<ITableMetadataAccessor>, TableMetadataAccessor);
    // Both orders are fixed here for the whole scan. RequestSorting is the order results come out in,
    // which is what the client asked for; SourcesSorting is the order sources are read in, which a source
    // index and therefore a scan cursor is meaningless without. Nothing may change either one later.
    YDB_READONLY(ERequestSorting, RequestSorting, ERequestSorting::NONE);
    YDB_READONLY(ESourcesSorting, SourcesSorting, ESourcesSorting::SourceIdAsc);
    YDB_READONLY(ui64, TabletId, 0);

    static ERequestSorting DeriveRequestSorting(const ERequestSorting requested, const EReaderClass readerClass) {
        // The plain reader has no unordered path, so a request that asked for no order reads ascending.
        if (readerClass == EReaderClass::Plain && requested == ERequestSorting::NONE) {
            return ERequestSorting::ASC;
        }
        return requested;
    }

    ESourcesSorting DeriveSourcesSorting() const {
        switch (RequestSorting) {
            case ERequestSorting::ASC:
                return ESourcesSorting::FirstPkAsc;
            case ERequestSorting::DESC:
                return ESourcesSorting::LastPkDesc;
            case ERequestSorting::NONE:
                if (!NeedDuplicateFiltering()) {
                    return ESourcesSorting::SourceIdAsc;
                }
                // Deduplication needs the sources in key order even though the results are unordered.
                switch (ReaderClass) {
                    case EReaderClass::Trivial:
                        return ESourcesSorting::LastPkAsc;
                    case EReaderClass::Simple:
                        // first_pk is strictly worse -- it enlarges the duplicates filter borders window --
                        // and stays only so the simple reader's numbers remain comparable with the old ones.
                        // Delete this case together with the simple reader.
                        return ESourcesSorting::FirstPkAsc;
                    case EReaderClass::Plain:
                        // Plain reader does not support deduplication, so we must not get here
                        break;
                }
        }
        AFL_VERIFY(false)("request_sorting", (ui64)RequestSorting)("reader_class", (ui64)ReaderClass);
        return ESourcesSorting::SourceIdAsc;
    }

public:
    ui64 TxId = 0;
    ui64 ScanId = 0;
    std::optional<ui64> LockId;
    std::optional<ui32> LockNodeId;
    std::optional<NKikimrDataEvents::ELockMode> LockMode;
    std::shared_ptr<NOlap::TPKRangesFilter> PKRangesFilter;
    NYql::NDqProto::EDqStatsMode StatsMode = NYql::NDqProto::EDqStatsMode::DQ_STATS_MODE_NONE;
    EScanGroupedMemoryLimiterOperator GroupedMemoryLimiterOperator = EScanGroupedMemoryLimiterOperator::Scan;
    std::shared_ptr<NLWTrace::TOrbit> Orbit;
    bool readNonconflictingPortions;
    bool readConflictingPortions;
    // portions that the current tx has written
    std::optional<THashSet<TInsertWriteId>> ownPortions;

    bool NeedDuplicateFiltering() const {
        AFL_VERIFY(TableMetadataAccessor);
        return DeduplicationEnabled && TableMetadataAccessor->NeedDuplicateFiltering();
    }

    TString GetLockName() const {
        if (TxId != 0 && ScanId != 0) {
            // proper kqp scan
            return TStringBuilder() << "scan:" << TxId << ":" << ScanId;
        } else {
            // internal scan
            AFL_VERIFY(!ScanIdentifier.empty());
            return TStringBuilder() << "scan:" << GetScanIdentifier();
        }
    }

    // List of columns
    std::vector<ui32> ColumnIds;

    const std::shared_ptr<IScanCursor>& GetScanCursorVerified() const {
        AFL_VERIFY(ScanCursor);
        return *ScanCursor;
    }

    void SetScanCursor(const std::shared_ptr<IScanCursor>& cursor) {
        AFL_VERIFY(!ScanCursor);
        ScanCursor = cursor;
    }

    void SetLock(std::optional<ui64> lockId, std::optional<ui32> lockNodeId, std::optional<NKikimrDataEvents::ELockMode> lockMode,
        const NColumnShard::TLockFeatures* lock, const bool readOnlyConflicts) {
        LockId = lockId;
        LockNodeId = lockNodeId;
        LockMode = lockMode;
        auto snapshotIsolation =
            lockId.has_value() && lockMode.value_or(NKikimrDataEvents::OPTIMISTIC) == NKikimrDataEvents::OPTIMISTIC_SNAPSHOT_ISOLATION;

        readNonconflictingPortions = !readOnlyConflicts;

        // do not check conflicts for Snapshot isolated txs or txs with no lock
        readConflictingPortions = (LockId.has_value() && !snapshotIsolation) || readOnlyConflicts;

        if (lock != nullptr && lock->GetWriteOperations().size() > 0) {
            ownPortions = THashSet<TInsertWriteId>();
            for (auto& writeOperation : lock->GetWriteOperations()) {
                for (auto insertWriteId : writeOperation->GetInsertWriteIds()) {
                    ownPortions->emplace(insertWriteId);
                }
            }
        }

        // we want to read something, don't we?
        AFL_VERIFY(readNonconflictingPortions || readConflictingPortions);
        // we do not have cases (at the moment) when we need to read only conflicts for a scan with no transaction
        if (!LockId.has_value()) {
            AFL_VERIFY(!readOnlyConflicts);
        }
    }

    TReadDescription(const ui64 tabletId, const TSnapshot& snapshot, const ERequestSorting requestSorting, const bool deduplicationEnabled,
        const EReaderClass readerClass, const std::shared_ptr<ITableMetadataAccessor>& tableMetadataAccessor,
        const std::optional<ESourcesSorting> sourcesSortingFromCursor)
        : Snapshot(snapshot)
        , DeduplicationEnabled(deduplicationEnabled)
        , ReaderClass(readerClass)
        , TableMetadataAccessor(tableMetadataAccessor)
        , RequestSorting(DeriveRequestSorting(requestSorting, readerClass))
        // sourcesSortingFromCursor comes from a resumed scan's cursor: that scan's order is the one its
        // source indexes were assigned in, so this one repeats it instead of deriving its own.
        , SourcesSorting(sourcesSortingFromCursor.value_or(DeriveSourcesSorting()))
        , TabletId(tabletId)
        , PKRangesFilter(std::make_shared<TPKRangesFilter>(TPKRangesFilter::BuildEmpty()))
    {
        AFL_VERIFY(TableMetadataAccessor);
    }

    void SetProgram(TProgramContainer&& value) {
        Program = std::move(value);
    }

    const TSnapshot& GetSnapshot() const {
        return Snapshot;
    }

    const TProgramContainer& GetProgram() const {
        return Program;
    }
};

}   // namespace NKikimr::NOlap::NReader
