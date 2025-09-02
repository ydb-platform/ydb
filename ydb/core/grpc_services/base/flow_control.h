#pragma once
#include <util/generic/queue.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

namespace NKikimr::NGRpcService {

class TRpcFlowControlState {
public:
    TRpcFlowControlState(ui64 inflightLimitBytes)
        : InflightLimitBytes_(inflightLimitBytes) {}

    void PushResponse(ui64 responseSizeBytes) {
        ResponseSizeQueue_.push(responseSizeBytes);
        TotalResponsesSize_ += responseSizeBytes;
    }

    void PopResponse() {
        Y_ENSURE(!ResponseSizeQueue_.empty());
        TotalResponsesSize_ -= ResponseSizeQueue_.front();
        ResponseSizeQueue_.pop();
    }

    size_t QueueSize() const {
        return ResponseSizeQueue_.size();
    }

    i64 FreeSpaceBytes() const { // Negative value temporarily stops data evaluation in DQ graph
        return static_cast<i64>(InflightLimitBytes_) - static_cast<i64>(TotalResponsesSize_);
    }

    ui64 InflightBytes() const {
        return TotalResponsesSize_;
    }

    ui64 InflightLimitBytes() const {
        return InflightLimitBytes_;
    }

private:
    const ui64 InflightLimitBytes_;

    TQueue<ui64> ResponseSizeQueue_;
    ui64 TotalResponsesSize_ = 0;
};

} // namespace NKikimr::NGRpcService
