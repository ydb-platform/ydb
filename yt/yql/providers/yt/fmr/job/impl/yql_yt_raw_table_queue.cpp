#include "yql_yt_raw_table_queue.h"
#include <yql/essentials/utils/yql_panic.h>

namespace NYql::NFmr {

TFmrRawTableQueue::TFmrRawTableQueue(ui64 numberOfInputs, const TFmrRawTableQueueSettings& settings)
    : InputStreamsNum_(numberOfInputs), Settings_(settings)
{
}

void TFmrRawTableQueue::AddRow(TBuffer&& rowContent) {
    YQL_ENSURE(rowContent.size() <= Settings_.MaxInflightBytes, "Row size is too large");
    with_lock(QueueMutex_) {
        QueueInflightBytesCondVar_.Wait(QueueMutex_, [&] {
            return CurInflightBytes_ + rowContent.size() < Settings_.MaxInflightBytes || ExceptionMessage_;
        });

        if (ExceptionMessage_) {
            return;
        }
        CurInflightBytes_ += rowContent.size();
        RowsQueue_.push(std::move(rowContent));
        QueueSizeCondVar_.BroadCast();
    }
}

TMaybe<TBuffer> TFmrRawTableQueue::PopRow() {
    with_lock(QueueMutex_) {
        QueueSizeCondVar_.Wait(QueueMutex_, [&] {
            return !RowsQueue_.empty() || IsFinished() || ExceptionMessage_;
        });
        if (ExceptionMessage_) {
            ythrow yexception() << ExceptionMessage_;
        }
        if (IsFinished()) {
            return Nothing();
        }
        auto row = RowsQueue_.front();
        RowsQueue_.pop();
        CurInflightBytes_ -= row.size();
        QueueInflightBytesCondVar_.BroadCast();
        return row;
    }
}

void TFmrRawTableQueue::NotifyInputFinished(ui64 input_id) {
    with_lock(QueueMutex_) {
        if (FinishedInputs_.contains(input_id)) {
            ExceptionMessage_ = TStringBuilder() << "Input stream with id" << input_id << " is marked as already finished";
        }
        FinishedInputs_.insert(input_id);
        QueueSizeCondVar_.BroadCast();
    }
}

bool TFmrRawTableQueue::IsFinished() const {
    return (RowsQueue_.empty() && FinishedInputs_.size() == InputStreamsNum_);
}

void TFmrRawTableQueue::SetException(const TString& exceptionMessage) {
    with_lock(QueueMutex_) {
        ExceptionMessage_ = exceptionMessage;
    }
    QueueInflightBytesCondVar_.BroadCast();
    QueueSizeCondVar_.BroadCast();
}

} // namespace NYql::NFmr
