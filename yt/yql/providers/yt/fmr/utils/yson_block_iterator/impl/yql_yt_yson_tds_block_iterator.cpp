#include "yt/yql/providers/yt/fmr/utils/yson_block_iterator/impl/yql_yt_yson_tds_block_iterator.h"

#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

#include <util/stream/mem.h>
#include <util/generic/hash_set.h>

namespace NYql::NFmr {

TTDSBlockIterator::TTDSBlockIterator(
    TString tableId,
    std::vector<TTableRange> tableRanges,
    ITableDataService::TPtr tableDataService,
    std::vector<TString> keyColumns,
    std::vector<ESortOrder> sortOrders,
    std::vector<TString> neededColumns,
    TString serializedColumnGroupsSpec,
    TMaybe<bool> isFirstRowKeysInclusive,
    TMaybe<TString> firstRowKeys,
    TMaybe<TString> lastRowKeys
)
    : TableId_(std::move(tableId))
    , TableRanges_(std::move(tableRanges))
    , TableDataService_(std::move(tableDataService))
    , KeyColumns_(std::move(keyColumns))
    , SortOrders_(std::move(sortOrders))
    , NeededColumns_(std::move(neededColumns))
    , SerializedColumnGroupsSpec_(std::move(serializedColumnGroupsSpec))
{
    if (SortOrders_.empty()) {
        SortOrders_.assign(KeyColumns_.size(), ESortOrder::Ascending);
    }
    if (SortOrders_.size() != KeyColumns_.size()) {
        ythrow yexception() << "SortOrders and KeyColumns sizes are different";
    }

    if (firstRowKeys) {
        FirstBound_ = TFmrTableKeysBoundary(*firstRowKeys, KeyColumns_, SortOrders_);
        Y_ENSURE(isFirstRowKeysInclusive.Defined(), "isFirstRowKeysInclusive must be defined for First Bound");
        IsFirstBoundInclusive_ = *isFirstRowKeysInclusive;
    }
    if (lastRowKeys) {
        LastBound_ = TFmrTableKeysBoundary(*lastRowKeys, KeyColumns_, SortOrders_);
    }

    SetMinChunkInNewRange();
} // namespace NYql::NFmr

TTDSBlockIterator::~TTDSBlockIterator() = default;

void TTDSBlockIterator::SetMinChunkInNewRange() {
    if (CurrentRange_ < TableRanges_.size()) {
        CurrentChunk_ = TableRanges_[CurrentRange_].MinChunk;
    }
}

bool TTDSBlockIterator::RowInKeyBounds(const TString& blob, const TRowIndexMarkup& row) const {
    if (FirstBound_) {
        int c = CompareKeyRowsAcrossYsonBlocks(
            blob,
            row,
            FirstBound_->Row,
            FirstBound_->Markup,
            SortOrders_
        );
        if (c < 0) { // if row < first bound
            return false;
        } else if (!IsFirstBoundInclusive_ && c == 0) { // if row == first bound
            return false;
        }
    }
    if (LastBound_) {
        int c = CompareKeyRowsAcrossYsonBlocks(
            blob,
            row,
            LastBound_->Row,
            LastBound_->Markup,
            SortOrders_
        );
        if (c > 0) { // if row > last bound
            return false;
        }
    }
    return true;
}

bool TTDSBlockIterator::NextBlock(TIndexedBlock& out) {
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

        const TString group = GetTableDataServiceGroup(TableId_, range.PartId);

        THashSet<TString> allNeeded;
        for (const auto& c : NeededColumns_) {
            allNeeded.insert(c);
        }
        for (const auto& c : KeyColumns_) {
            allNeeded.insert(c);
        }

        std::vector<TString> groupNamesToRead;
        if (SerializedColumnGroupsSpec_.empty()) {
            groupNamesToRead.push_back(TString());
        } else {
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

            groupNamesToRead.assign(groupNames.begin(), groupNames.end());
        }

        std::vector<TString> groupYsons;
        groupYsons.reserve(groupNamesToRead.size());
        for (const auto& gname : groupNamesToRead) {
            const TString dataChunkId = GetTableDataServiceChunkId(CurrentChunk_, gname);
            auto data = TableDataService_->Get(group, dataChunkId).GetValueSync();
            groupYsons.emplace_back(data.Defined() ? std::move(*data) : TString());
        }

        const TString unionYson = GetYsonUnion(groupYsons, NeededColumns_);

        TParserFragmentListIndex parser(unionYson, KeyColumns_);
        parser.Parse();
        const auto& rows = parser.GetRows();

        out.Data = unionYson;
        if (FirstBound_ || LastBound_) {
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

}
