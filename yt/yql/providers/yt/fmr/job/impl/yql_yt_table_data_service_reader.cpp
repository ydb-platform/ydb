#include "yql_yt_table_data_service_reader.h"
#include <library/cpp/threading/future/wait/wait.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/utils/yql_yt_table_data_service_key.h>

namespace NYql::NFmr {

TFmrTableDataServiceReader::TFmrTableDataServiceReader(
    const TString& tableId,
    const std::vector<TTableRange>& tableRanges,
    ITableDataService::TPtr tableDataService,
    const std::vector<TString>& neededColumns,
    const TString& columnGroupSpec,
    const TFmrReaderSettings& settings
)
    : TableId_(tableId),
    TableRanges_(tableRanges),
    TableDataService_(tableDataService),
    ReadAheadChunks_(settings.ReadAheadChunks),
    NeededColumns_(neededColumns),
    ColumnGroupSpec_(GetColumnGroupsFromSpec(columnGroupSpec))
{
    SetMinChunkInNewRange();
    ReadAhead();

}

// TODO  - написать логику с NeededColumns

size_t TFmrTableDataServiceReader::DoRead(void* buf, size_t len) {
    ui64 totalRead = 0;
    char* output = static_cast<char*>(buf);
    while (len > 0) {
        ui64 available = DataBuffer_.size() - CurrentPosition_;
        if (available > 0) {
            ui64 toCopy = std::min(available, len);
            auto start = DataBuffer_.Begin() + CurrentPosition_;
            auto end = start + toCopy;
            std::copy(start, end, output);

            CurrentPosition_ += toCopy;
            output += toCopy;
            len -= toCopy;
            totalRead += toCopy;
        } else if (!PendingChunks_.empty()) {
            auto chunk = PendingChunks_.front();
            TMaybe<TString> data;
            try {
                data = chunk.Data.GetValueSync();
            } catch (...) {
                ythrow yexception() << "Error reading chunk: " << chunk.Meta.ToString() << "Error: " << CurrentExceptionMessage();
            }
            if (data) {
                DataBuffer_.Assign(data->data(), data->size());
            } else {
                ythrow yexception() << "No data for chunk:" << chunk.Meta.ToString();
            }

            PendingChunks_.pop();
            CurrentPosition_ = 0;
            available = DataBuffer_.size();
            ReadAhead();
        } else {
            break;
        }
    }
    return totalRead;
}

void TFmrTableDataServiceReader::ReadAhead() {
    while (PendingChunks_.size() < ReadAheadChunks_) {
        if (CurrentRange_ == TableRanges_.size()) {
            break;
        }
        auto currentPartId = TableRanges_[CurrentRange_].PartId;
        if (CurrentChunk_ < TableRanges_[CurrentRange_].MaxChunk) {
            auto tableDataServiceGetFuture = GetTableDataServiceValueFuture(currentPartId, CurrentChunk_);
            PendingChunks_.push({.Data=tableDataServiceGetFuture, .Meta={TableId_, currentPartId, CurrentChunk_}});
            CurrentChunk_++;
        } else {
            CurrentRange_++;
            SetMinChunkInNewRange();
        }
    }
}

NThreading::TFuture<TMaybe<TString>> TFmrTableDataServiceReader::GetTableDataServiceValueFuture(const TString& partId, ui64 chunkNum) {
    const auto tableDataServiceGroup = GetTableDataServiceGroup(TableId_, partId);
    if (ColumnGroupSpec_.IsEmpty()) {
        // Column group spec is not set, so table data service has single value with all columns.
        auto tableDataServiceChunkId = GetTableDataServiceChunkId(chunkNum, TString());
        return TableDataService_->Get(tableDataServiceGroup, tableDataServiceChunkId);
    }

    std::vector<NThreading::TFuture<TMaybe<TString>>> getTableDataServiceColumnGroupValueFutures;
    for (auto& [groupName, cols]: ColumnGroupSpec_.ColumnGroups) {
        auto tableDataServiceChunkId = GetTableDataServiceChunkId(chunkNum, groupName);
        getTableDataServiceColumnGroupValueFutures.emplace_back(TableDataService_->Get(tableDataServiceGroup, tableDataServiceChunkId));
    }

    if (!ColumnGroupSpec_.DefaultColumnGroupName.empty()) {
        getTableDataServiceColumnGroupValueFutures.emplace_back(TableDataService_->Get(tableDataServiceGroup, GetTableDataServiceChunkId(chunkNum, ColumnGroupSpec_.DefaultColumnGroupName)));

    }

    return NThreading::WaitExceptionOrAll(getTableDataServiceColumnGroupValueFutures).Apply([&getTableDataServiceColumnGroupValueFutures] (const auto& f) {
        f.GetValue(); // rethrow error if any
        std::vector<TString> columnGroupsYsonValues;
        for (auto& future: getTableDataServiceColumnGroupValueFutures) {
            TMaybe<TString> colGroupYsonValue = future.GetValue();
            YQL_ENSURE(colGroupYsonValue.Defined());
            columnGroupsYsonValues.emplace_back(*colGroupYsonValue);
        }
        return NThreading::MakeFuture<TMaybe<TString>>(GetYsonUnion(columnGroupsYsonValues));
    });

}

void TFmrTableDataServiceReader::SetMinChunkInNewRange() {
    if (CurrentRange_ < TableRanges_.size()) {
        CurrentChunk_ = TableRanges_[0].MinChunk;
    }
}

bool TFmrTableDataServiceReader::Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) {
    return false;
}

void TFmrTableDataServiceReader::ResetRetries() { }

bool TFmrTableDataServiceReader::HasRangeIndices() const {
    return false;
}

TString TFmrTableDataServiceReader::TFmrChunkMeta::ToString() const {
    return TStringBuilder() << TableId << "_" << PartId << ":" << Chunk;
}

} // namespace NYql::NFmr
