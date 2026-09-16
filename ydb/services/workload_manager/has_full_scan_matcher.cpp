#include "has_full_scan_matcher.h"

#include <ydb/core/base/path.h>


namespace NKikimr::NWorkloadManager {

namespace {

// Full-scan detection is purely a "no key filter" check. LIMIT is intentionally
// ignored: `WHERE filter LIMIT N` with a zero-selectivity filter scans the whole
// table at runtime, so treating LIMIT as an escape hatch would leak real full
// scans past the classifier.
bool IsFullScanTableOp(const NKqpProto::TKqpPhyTableOperation& op) {
    if (op.HasReadRange()) {
        // Row-store ReadRange: full scan iff both key bounds are empty.
        const auto& range = op.GetReadRange().GetKeyRange();
        return range.GetFrom().ValuesSize() == 0 && range.GetTo().ValuesSize() == 0;
    }
    if (op.HasReadRanges()) {
        // Multi-range ReadRanges: full scan iff KeyRanges param name is empty.
        return op.GetReadRanges().GetKeyRanges().GetParamName().empty();
    }
    if (op.HasReadOlapRange()) {
        // OLAP ReadRanges: same rule — empty param name means "no key filter".
        return op.GetReadOlapRange().GetKeyRanges().GetParamName().empty();
    }
    return false;
}

bool IsFullScanSource(const NKqpProto::TKqpReadRangesSource& source) {
    if (source.HasKeyRange()) {
        const auto& range = source.GetKeyRange();
        return range.GetFrom().ValuesSize() == 0 && range.GetTo().ValuesSize() == 0;
    }
    if (source.HasRanges()) {
        return source.GetRanges().GetParamName().empty();
    }
    // Neither KeyRange nor Ranges set — no filter, so full scan.
    return true;
}

}  // anonymous namespace


bool MatchesFullScan(const std::optional<NResourcePool::TRegexPredicate>& predicate,
                   const NKqpProto::TKqpPhyQuery& phyQuery) {
    if (!predicate) {
        return true;
    }

    for (const auto& tx : phyQuery.GetTransactions()) {
        for (const auto& stage : tx.GetStages()) {
            for (const auto& op : stage.GetTableOps()) {
                if (!predicate->Match(CanonizePath(op.GetTable().GetPath()))) {
                    continue;
                }
                if (IsFullScanTableOp(op)) {
                    return true;
                }
            }

            for (const auto& source : stage.GetSources()) {
                if (!source.HasReadRangesSource()) {
                    continue;
                }
                const auto& rs = source.GetReadRangesSource();
                if (!predicate->Match(CanonizePath(rs.GetTable().GetPath()))) {
                    continue;
                }
                if (IsFullScanSource(rs)) {
                    return true;
                }
            }
        }
    }

    return false;
}

}  // namespace NKikimr::NWorkloadManager
