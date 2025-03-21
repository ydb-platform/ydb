#include "yql_yt_table_data_service_reader.h"
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>

namespace NYql::NFmr {

TFmrTableDataServiceReader::TFmrTableDataServiceReader(
    const TString& tableId,
    const std::vector<TTableRange>& tableRanges,
    ITableDataService::TPtr tableDataService,
    const TFmrTableDataServiceReaderSettings& settings
)
    : TableId_(tableId),
    TableRanges_(tableRanges),
    TableDataService_(tableDataService),
    ReadAheadChunks_(settings.ReadAheadChunks)
{
    ReadAhead();
}

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
                ythrow yexception() << "Error reading chunk: " << chunk.Meta << "Error: " << CurrentExceptionMessage();
            }
            if (data) {
                DataBuffer_.Assign(data->data(), data->size());
            } else {
                ythrow yexception() << "No data for chunk:" << chunk.Meta;
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
            auto key = GetTableDataServiceKey(TableId_, currentPartId, CurrentChunk_);
            PendingChunks_.push({.Data=TableDataService_->Get(key), .Meta={TableId_, currentPartId, CurrentChunk_}});
            CurrentChunk_++;
        } else {
            CurrentRange_++;
        }
    }
}

bool TFmrTableDataServiceReader::Retry(const TMaybe<ui32>&, const TMaybe<ui64>&, const std::exception_ptr&) {
    return false;
}

void TFmrTableDataServiceReader::ResetRetries() { }

bool TFmrTableDataServiceReader::HasRangeIndices() const {
    return false;
}

} // namespace NYql::NFmr
