#include "yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h"

#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

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
    }

    if (SerializedColumnGroupsSpec_.empty()) {
        GroupNamesToRead_.push_back(TString());
    } else {
        THashSet<TString> allNeeded;
        for (const auto& c : NeededColumns_) {
            allNeeded.insert(c);
        }
        for (const auto& c : KeyColumns_) {
            allNeeded.insert(c);
        }

        const auto spec = GetColumnGroupsFromSpec(SerializedColumnGroupsSpec_);
        THashSet<TString> groupNames;

        auto findGroupForColumn = [&spec](const TString& col) -> TString {
            for (const auto& [groupName, cols] : spec.ColumnGroups) {
                if (cols.contains(col)) {
                    return groupName;
                }
            }
            return spec.DefaultColumnGroupName;
        };

        for (const auto& col : allNeeded) {
            groupNames.insert(findGroupForColumn(col));
        }

        GroupNamesToRead_.assign(groupNames.begin(), groupNames.end());
    }

    SetMinChunkInNewRange();

    PrefetchRange_ = CurrentRange_;
    PrefetchChunk_ = CurrentChunk_;

    FillPrefetchQueue();
}

TTableDataServiceBlockIterator::~TTableDataServiceBlockIterator() = default;

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

bool TTableDataServiceBlockIterator::RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const {
    if (FirstBoundary_) {
        int c = CompareKeyRowsAcrossYsonBlocks(
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
        int c = CompareKeyRowsAcrossYsonBlocks(
            blob,
            row,
            LastBoundary_->Row,
            LastBoundary_->Markup,
            SortOrders_
        );
        if (c > 0) { // if row > last boundary
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
