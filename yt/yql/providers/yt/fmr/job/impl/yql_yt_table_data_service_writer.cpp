#include "yql_yt_table_data_service_writer.h"
#include <library/cpp/threading/future/wait/wait.h>
#include <util/string/join.h>
#include <yql/essentials/utils/yql_panic.h>


namespace NYql::NFmr {

TFmrTableDataServiceWriter::TFmrTableDataServiceWriter(
    const TString& tableId,
    const TString& partId,
    ITableDataService::TPtr tableDataService,
    const TFmrWriterSettings& settings
)
    : TableId_(tableId),
    PartId_(partId),
    TableDataService_(tableDataService),
    ChunkSize_(settings.ChunkSize),
    MaxInflightChunks_(settings.MaxInflightChunks),
    MaxRowWeight_(settings.MaxRowWeight)
{
    YQL_ENSURE(MaxRowWeight_ >= ChunkSize_);
}

void TFmrTableDataServiceWriter::DoWrite(const void* buf, size_t len) {
    TableContent_.Append(static_cast<const char*>(buf), len);
}

void TFmrTableDataServiceWriter::NotifyRowEnd()  {
    ++Rows_;
    if (TableContent_.size() >= MaxRowWeight_) {
        ythrow yexception() << "Current row size: " << TableContent_.size() << " is larger than max row weight: " << MaxRowWeight_;
    }
    if (TableContent_.size() >= ChunkSize_) {
        PutRows();
    }
}

void TFmrTableDataServiceWriter::DoFlush() {
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

void TFmrTableDataServiceWriter::PutRows() {
    with_lock(State_->Mutex) {
        State_->CondVar.Wait(State_->Mutex, [&] {
            return State_->CurInflightChunks < MaxInflightChunks_;
        });
        ++State_->CurInflightChunks;
    }
    TString chunkKey = GetTableDataServiceKey(TableId_, PartId_, ChunkCount_);
    TableDataService_->Put(chunkKey, TString(TableContent_.Data(), TableContent_.Size())).Subscribe(
        [weakState = std::weak_ptr(State_)] (const auto& putFuture) mutable {
            std::shared_ptr<TFmrWriterState> state = weakState.lock();
            if (state) {
                with_lock(state->Mutex) {
                    --state->CurInflightChunks;
                    try {
                        putFuture.GetValue();
                    } catch (...) {
                        if (!state->Exception) {
                            state->Exception = std::current_exception();
                        }
                    }
                    state->CondVar.Signal();
                }
            }
        }
    );
    ++ChunkCount_;
    DataWeight_ += TableContent_.Size();
    TableContent_.Clear();
}

TTableStats TFmrTableDataServiceWriter::GetStats() {
    return TTableStats{
        .Chunks = ChunkCount_,
        .Rows = Rows_,
        .DataWeight = DataWeight_,
    };
}

} // namespace NYql::NFmr
