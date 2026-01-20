#include "yql_yt_yson_tds_block_iterator.h"

#include <yt/yql/providers/yt/fmr/utils/yql_yt_column_group_helpers.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_parser_fragment_list_index.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

#include <util/stream/mem.h>
#include <util/generic/hash_set.h>

namespace NYql::NFmr {

TTDSBlockIterator::TTDSBlockIterator(
    TString tableId,
    TVector<TTableRange> tableRanges,
    ITableDataService::TPtr tableDataService,
    TVector<TString> keyColumns,
    TVector<TString> neededColumns,
    TString serializedColumnGroupsSpec
)
    : TableId_(std::move(tableId))
    , TableRanges_(std::move(tableRanges))
    , TableDataService_(std::move(tableDataService))
    , KeyColumns_(std::move(keyColumns))
    , NeededColumns_(std::move(neededColumns))
    , SerializedColumnGroupsSpec_(std::move(serializedColumnGroupsSpec))
{
    Y_ENSURE(TableDataService_);
    SetMinChunkInNewRange();
}

TTDSBlockIterator::~TTDSBlockIterator() = default;

void TTDSBlockIterator::SetMinChunkInNewRange() {
    if (CurrentRange_ < TableRanges_.size()) {
        CurrentChunk_ = TableRanges_[CurrentRange_].MinChunk;
    }
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

        TVector<TString> groupNamesToRead;
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

        TVector<TString> groupYsons;
        groupYsons.reserve(groupNamesToRead.size());
        for (const auto& gname : groupNamesToRead) {
            const TString dataChunkId = GetTableDataServiceChunkId(CurrentChunk_, gname);
            auto data = TableDataService_->Get(group, dataChunkId).GetValueSync();
            Y_ENSURE(data.Defined(), TStringBuilder() << "No data for chunkId=" << dataChunkId << " group=" << group);
            groupYsons.emplace_back(std::move(*data));
        }

        const TString unionYson = GetYsonUnion(groupYsons, NeededColumns_);

        TParserFragmentListIndex parser(unionYson, KeyColumns_);
        parser.Parse();
        const auto& rows = parser.GetRows();

        out.Data = unionYson;
        out.Rows = rows;

        ++CurrentChunk_;
        return true;
    }
}

} // namespace NYql::NFmr
