#include "yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h"

#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

#include <yql/essentials/utils/log/log.h>
#include <util/stream/mem.h>
#include <util/generic/hash_set.h>

namespace NYql::NFmr {

TTableDataServiceBlockIterator::TTableDataServiceBlockIterator(
    TString tableId,
    std::vector<TTableRange> tableRanges,
    ITableDataService::TPtr tableDataService,
    std::vector<TString> keyColumns,
    std::vector<ESortOrder> sortOrders,
    std::vector<TString> neededColumns,
    TString serializedColumnGroupsSpec,
    TMaybe<bool> isFirstRowKeysInclusive,
    TMaybe<bool> isLastRowKeysInclusive,
    TMaybe<TString> firstRowKeys,
    TMaybe<TString> lastRowKeys,
    ui64 readAheadChunks
)
    : TableId_(std::move(tableId))
    , TableRanges_(std::move(tableRanges))
    , TableDataService_(std::move(tableDataService))
    , KeyColumns_(std::move(keyColumns))
    , SortOrders_(std::move(sortOrders))
    , NeededColumns_(std::move(neededColumns))
    , SerializedColumnGroupsSpec_(std::move(serializedColumnGroupsSpec))
    , ReadAheadChunks_(readAheadChunks)
{
    if (SortOrders_.empty()) {
        SortOrders_.assign(KeyColumns_.size(), ESortOrder::Ascending);
    }
    if (SortOrders_.size() != KeyColumns_.size()) {
        ythrow yexception() << "SortOrders and KeyColumns sizes are different";
    }

    if (firstRowKeys) {
        FirstBoundary_ = TFmrTableKeysBoundary(*firstRowKeys, KeyColumns_, SortOrders_);
        Y_ENSURE(isFirstRowKeysInclusive.Defined(), "isFirstRowKeysInclusive must be defined for First Boundary");
        IsFirstBoundInclusive_ = *isFirstRowKeysInclusive;
    }
    if (lastRowKeys) {
        LastBoundary_ = TFmrTableKeysBoundary(*lastRowKeys, KeyColumns_, SortOrders_);
        Y_ENSURE(isLastRowKeysInclusive.Defined(), "isLastRowKeysInclusive must be defined for Last Boundary");
        IsLastBoundInclusive_ = *isLastRowKeysInclusive;
    }

    if (SerializedColumnGroupsSpec_.empty()) {
        GroupNamesToRead_.push_back(TString());
    } else {
        const auto spec = GetColumnGroupsFromSpec(SerializedColumnGroupsSpec_);

        if (NeededColumns_.empty()) {
            for (const auto& [groupName, cols] : spec.ColumnGroups) {
                GroupNamesToRead_.push_back(groupName);
            }
            GroupNamesToRead_.push_back(spec.DefaultColumnGroupName);
        } else {
            THashSet<TString> allNeeded(NeededColumns_.begin(), NeededColumns_.end());
            allNeeded.insert(KeyColumns_.begin(), KeyColumns_.end());

            THashSet<TString> groupNames;
            for (const auto& col : allNeeded) {
                groupNames.insert(FindGroupForColumn(col, spec));
            }
            GroupNamesToRead_.assign(groupNames.begin(), groupNames.end());
        }
    }

    SetMinChunkInNewRange();

    PrefetchRange_ = CurrentRange_;
    PrefetchChunk_ = CurrentChunk_;

    FillPrefetchQueue();
}

TTableDataServiceBlockIterator::~TTableDataServiceBlockIterator() = default;

TString TTableDataServiceBlockIterator::FindGroupForColumn(const TString& col, const TParsedColumnGroupSpec& spec) {
    for (const auto& [groupName, cols] : spec.ColumnGroups) {
        if (cols.contains(col)) {
            return groupName;
        }
    }
    return spec.DefaultColumnGroupName;
}

void TTableDataServiceBlockIterator::SetMinChunkInNewRange() {
    if (CurrentRange_ < TableRanges_.size()) {
        CurrentChunk_ = TableRanges_[CurrentRange_].MinChunk;
    }
}

bool TTableDataServiceBlockIterator::TrySchedulePrefetch() {
    while (PrefetchRange_ < TableRanges_.size()) {
        const auto& range = TableRanges_[PrefetchRange_];
        if (PrefetchChunk_ < range.MaxChunk) {
            const TString group = GetTableDataServiceGroup(TableId_, range.PartId);
            TPrefetchEntry entry;
            entry.Futures.reserve(GroupNamesToRead_.size());
            for (const auto& gname : GroupNamesToRead_) {
                const TString dataChunkId = GetTableDataServiceChunkId(PrefetchChunk_, gname);
                YQL_CLOG(DEBUG, FastMapReduce) << "TTableDataServiceBlockIterator::Prefetch: group=" << group
                    << " chunkId=" << dataChunkId << " (gname='" << gname << "' chunk=" << PrefetchChunk_ << ")";
                entry.Futures.push_back(TableDataService_->Get(group, dataChunkId));
            }
            PrefetchQueue_.push_back(std::move(entry));
            ++PrefetchChunk_;
            return true;
        }
        ++PrefetchRange_;
        if (PrefetchRange_ < TableRanges_.size()) {
            PrefetchChunk_ = TableRanges_[PrefetchRange_].MinChunk;
        }
    }
    return false;
}

void TTableDataServiceBlockIterator::FillPrefetchQueue() {
    while (PrefetchQueue_.size() < ReadAheadChunks_) {
        if (!TrySchedulePrefetch()) {
            break;
        }
    }
}

namespace {

// A key column's value is considered null either because the row's binary yson genuinely omits
// it (IsValid()==false - e.g. a nullable column that a sparse writer drops rather than encoding
// as an explicit entity) or because it IS encoded, but as the explicit '#' entity marker (e.g.
// boundary blobs built from a NYT::TNode map, which always sets every key, and represents a NULL
// value as an entity rather than omitting the key).
bool IsNullColumnValue(TStringBuf blob, const TColumnOffsetRange& range) {
    if (!range.IsValid()) {
        return true;
    }
    return SliceRange(blob, range) == TStringBuf(&EntitySymbol, 1);
}

// FirstRowKeys/LastRowKeys boundaries are built by the coordinator from a task's chunk-stats
// key columns (e.g. [_yql_key_hash, ...ReduceBy] for MapReduce reduce tasks), which can be a
// strict PREFIX of the fuller key column set (e.g. [_yql_key_hash, ...SortBy]) this iterator
// parses actual rows with. TFmrTableKeysBoundary parses its own Row bytes with the SAME
// (fuller) key columns as the row markup, so any column beyond the boundary's real prefix comes
// out with an IsValid()==false range - not because the value is legitimately null, but because
// the boundary blob simply never encoded it. Comparing that as "boundary < row" (the usual
// null-handling rule) would incorrectly exclude real rows that land exactly on the boundary, so
// this comparison stops at the boundary's own valid prefix instead of delegating to the
// generic null-aware CompareKeyRowsAcrossYsonBlocks.
int CompareRowToBoundaryPrefix(
    TStringBuf rowBlob,
    const TRowIndexMarkup& row,
    TStringBuf boundaryBlob,
    const TRowIndexMarkup& boundaryMarkup,
    const std::vector<ESortOrder>& sortOrders
) {
    for (ui64 colIdx = 0; colIdx + 1 < boundaryMarkup.size(); ++colIdx) {
        const auto& boundaryRange = boundaryMarkup[colIdx];
        if (!boundaryRange.IsValid()) {
            // Boundary doesn't encode this column (or any further one, since it's a prefix) -
            // no constraint from here on.
            break;
        }
        const auto& rowRange = row[colIdx];

        // Both sides null (whichever way each happens to encode it) - equal at this column,
        // keep comparing the remaining columns instead of declaring the row out of bounds.
        const bool rowIsNull = IsNullColumnValue(rowBlob, rowRange);
        const bool boundaryIsNull = IsNullColumnValue(boundaryBlob, boundaryRange);
        if (rowIsNull && boundaryIsNull) {
            continue;
        }
        if (rowIsNull) {
            return -1;
        }
        if (boundaryIsNull) {
            return 1;
        }

        int result = CompareYsonValuesImpl(SliceRange(rowBlob, rowRange), SliceRange(boundaryBlob, boundaryRange));
        if (sortOrders[colIdx] == ESortOrder::Descending) {
            result = -result;
        }
        if (result != 0) {
            return result;
        }
    }
    return 0;
}

} // namespace

bool TTableDataServiceBlockIterator::RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const {
    if (FirstBoundary_) {
        int c = CompareRowToBoundaryPrefix(
            blob,
            row,
            FirstBoundary_->Row,
            FirstBoundary_->Markup,
            SortOrders_
        );
        if (c < 0) { // if row < first boundary
            return false;
        } else if (!IsFirstBoundInclusive_ && c == 0) { // if row == first boundary
            return false;
        }
    }
    if (LastBoundary_) {
        int c = CompareRowToBoundaryPrefix(
            blob,
            row,
            LastBoundary_->Row,
            LastBoundary_->Markup,
            SortOrders_
        );
        if (c > 0) { // if row > last boundary
            return false;
        } else if (!IsLastBoundInclusive_ && c == 0) {
            return false;
        }
    }
    return true;
}

bool TTableDataServiceBlockIterator::NextBlock(TIndexedBlock& out) {
    out = {};

    while (true) {
        if (CurrentRange_ >= TableRanges_.size()) {
            return false;
        }

        const auto& range = TableRanges_[CurrentRange_];
        if (CurrentChunk_ >= range.MaxChunk) {
            ++CurrentRange_;
            SetMinChunkInNewRange();
            continue;
        }

        std::vector<TString> groupYsons;
        groupYsons.reserve(GroupNamesToRead_.size());

        if (!PrefetchQueue_.empty()) {
            auto& entry = PrefetchQueue_.front();
            for (auto& future : entry.Futures) {
                auto data = future.GetValueSync();
                groupYsons.emplace_back(data.Defined() ? std::move(*data) : TString());
            }
            PrefetchQueue_.pop_front();
            FillPrefetchQueue();
        } else {
            const TString group = GetTableDataServiceGroup(TableId_, range.PartId);
            for (const auto& gname : GroupNamesToRead_) {
                const TString dataChunkId = GetTableDataServiceChunkId(CurrentChunk_, gname);
                auto data = TableDataService_->Get(group, dataChunkId).GetValueSync();
                groupYsons.emplace_back(data.Defined() ? std::move(*data) : TString());
            }
        }

        TString unionYson;
        if (groupYsons.size() == 1 && NeededColumns_.empty()) {
            unionYson = std::move(groupYsons[0]);
        } else {
            unionYson = GetYsonUnionRaw(groupYsons, NeededColumns_);
        }

        TParserFragmentListIndex parser(unionYson, KeyColumns_);
        parser.Parse();
        const auto& rows = parser.GetRows();

        out.Data = unionYson;
        if (FirstBoundary_ || LastBoundary_) {
            std::vector<TRowIndexMarkup> filtered;
            filtered.reserve(rows.size());
            for (const auto& r : rows) {
                if (RowInKeyBounds(out.Data, r)) {
                    filtered.push_back(r);
                }
            }
            out.Rows = std::move(filtered);
        } else {
            out.Rows = rows;
        }
        ++CurrentChunk_;
        return true;
    }
}

std::vector<ESortOrder> TTableDataServiceBlockIterator::GetSortOrder() {
    return SortOrders_;
}

} // namespace NYql::NFmr
