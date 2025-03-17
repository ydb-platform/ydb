#include "yql_yt_raw_table_reader.h"
#include <yql/essentials/utils/log/log.h>
#include <yt/yql/providers/yt/fmr/utils/table_data_service_key.h>

namespace NYql::NFmr {

TFmrRawTableReader::TFmrRawTableReader(
    const TString& tableId,
    const std::vector<TTableRange>& tableRanges,
    ITableDataService::TPtr tableDataService,
    const TFmrRawTableReaderSettings& settings
)
    : TableId_(tableId)
    , TableRanges_(tableRanges)
    , TableDataService_(tableDataService)
    , Settings_(settings)
{
    ReadAhead();
}

size_t TFmrRawTableReader::DoRead(void* buf, size_t len) {
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
                ythrow yexception() << "Error reading chunk:" << chunk.Meta << "Error: " << CurrentExceptionMessage();
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

void TFmrRawTableReader::ReadAhead() {
    while (PendingChunks_.size() < Settings_.ReadAheadChunks) {
        if (CurrentRange_ < TableRanges_.size()) {
            auto currentPartId = TableRanges_[CurrentRange_].PartId;
            if (CurrentChunk_ < TableRanges_[CurrentRange_].MaxChunk) {
                auto key = GetTableDataServiceKey(TableId_, currentPartId, CurrentChunk_);
                PendingChunks_.push({TableDataService_->Get(key), {TableId_, currentPartId, CurrentChunk_}});
                CurrentChunk_++;
            } else {
                CurrentRange_++;
            }
        }
        else {
            break;
        }
    }
}

} // namespace NYql::NFmr
