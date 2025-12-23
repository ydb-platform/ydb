#include "yql_yt_table_data_service_base_writer.h"
#include <library/cpp/threading/future/wait/wait.h>
#include <util/string/join.h>
#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NFmr {

TFmrTableDataServiceBaseWriter::TFmrTableDataServiceBaseWriter(
    const TString& tableId,
    const TString& partId,
    ITableDataService::TPtr tableDataService,
    const TString& columnGroupSpec,
    const TFmrWriterSettings& settings
)
    : TableId_(tableId),
    PartId_(partId),
    TableDataService_(tableDataService),
    ChunkSize_(settings.ChunkSize),
    MaxInflightChunks_(settings.MaxInflightChunks),
    MaxRowWeight_(settings.MaxRowWeight),
    ColumnGroupSpec_(GetColumnGroupsFromSpec(columnGroupSpec))
{
    YQL_ENSURE(MaxRowWeight_ >= ChunkSize_);
}

void TFmrTableDataServiceBaseWriter::DoWrite(const void* buf, size_t len) {
    Cerr << "DoWrite: " << len << Endl;
    TableContent_.Append(static_cast<const char*>(buf), len);
    Cerr << "DoWrite: table size = " << TableContent_.size() << Endl;
}

void TFmrTableDataServiceBaseWriter::NotifyRowEnd()  {
    if (TableContent_.size() > MaxRowWeight_) {
        ythrow yexception() << "Current row size: " << TableContent_.size() << " is larger than max row weight: " << MaxRowWeight_;
    }
    if (TableContent_.size() >= ChunkSize_) {
        PutRows();
    }
}

TTableChunkStats TFmrTableDataServiceBaseWriter::GetStats() {
    YQL_CLOG(DEBUG, FastMapReduce) << " Finished writing to table data service for table Id: " << TableId_ << " and part Id " << PartId_;
    return TTableChunkStats{.PartId = PartId_, .PartIdChunkStats = PartIdChunkStats_};
}

void TFmrTableDataServiceBaseWriter::DoFlush() {
    PutRows();
    with_lock(State_->Mutex) {
        if (State_->Exception) {
            std::rethrow_exception(State_->Exception);
        }
        State_->CondVar.Wait(State_->Mutex, [this] {
            return State_->CurInflightChunks == 0;
        });
    }
}

NThreading::TFuture<void> TFmrTableDataServiceBaseWriter::PutYsonByColumnGroups(const TString& currentYsonContent) {
    std::unordered_map<TString, TString> splittedYsonByColumnGroups;
    auto columnGroupSplitYsonResult = SplitYsonByColumnGroups(currentYsonContent, ColumnGroupSpec_);
    ui64 recordsCount = columnGroupSplitYsonResult.RecordsCount;
    CurrentChunkRows_ += recordsCount;
    splittedYsonByColumnGroups = columnGroupSplitYsonResult.SplittedYsonByColumnGroups;

    with_lock(State_->Mutex) {
        State_->CondVar.Wait(State_->Mutex, [&] {
            return State_->CurInflightChunks < MaxInflightChunks_;
        });
        State_->CurInflightChunks += splittedYsonByColumnGroups.size(); // Adding number of keys which we want to put to TableDataService to inflight
    }

    TVector<NThreading::TFuture<void>> result;
    result.reserve(splittedYsonByColumnGroups.size());
    for (auto& [groupName, columnGroupYsonContent]: splittedYsonByColumnGroups) {
        auto tableDataServiceGroup = GetTableDataServiceGroup(TableId_, PartId_);
        auto tableDataServiceChunkId = GetTableDataServiceChunkId(ChunkCount_, groupName);
        auto future = TableDataService_->Put(tableDataServiceGroup, tableDataServiceChunkId,columnGroupYsonContent).Subscribe(
            [weakState = std::weak_ptr(State_)] (const auto& putFuture) mutable {
                std::shared_ptr<TFmrWriterState> state = weakState.lock();
                if (state) {
                    with_lock(state->Mutex) {
                        --state->CurInflightChunks;
                        putFuture.GetValue();
                        state->CondVar.Signal();
                    }
                }
            }
        );
        result.push_back(future);
    }
    return NThreading::WaitExceptionOrAll(std::move(result));
}

void TFmrTableDataServiceBaseWriter::ClearTableData() {
    DataWeight_ += TableContent_.Size();
    ++ChunkCount_;
    CurrentChunkRows_ = 0;
    TableContent_.Clear();
}

} // namespace NYql::NFmr
