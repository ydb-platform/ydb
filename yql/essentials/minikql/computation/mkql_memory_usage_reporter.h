#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <functional>
#include <format>

namespace NKikimr::NMiniKQL {

class TMemoryUsageReporter {
public:
    using TReportCallback = std::function<void(ui64)>;
    using TPtr = std::shared_ptr<TMemoryUsageReporter>;
public:
    TMemoryUsageReporter(TReportCallback reportAllocateCallback, TReportCallback reportFreeCallback): ReportAllocateCallback_(reportAllocateCallback), ReportFreeCallback_(reportFreeCallback) {}
    void ReportAllocate(ui64 bytes) {
        // Cerr << "[MISHA][ALLOC]: " << bytes << ", Total: " << BytesAllocated_ << Endl;
        Y_ENSURE(ReportAllocateCallback_ != nullptr);
        ReportAllocateCallback_(bytes);
        BytesAllocated_ += bytes;
    }

    void ReportFree(ui64 bytes) {
        // Cerr << "[MISHA][FREE]: " << bytes << ", Total: " << BytesAllocated_ << Endl;
        Y_ENSURE(ReportFreeCallback_ != nullptr);
        ReportFreeCallback_(bytes);
        Y_ENSURE(BytesAllocated_ >= bytes, "Trying to free more bytes than allocated");
        BytesAllocated_ -= bytes;
    }

    ~TMemoryUsageReporter() {
        // used only for test purposes. Must be changed to 
        // ReportFreeCallback_(BytesAllocated_);
        // Cerr << "[MISHA][DESTR]: " << "Total: " << BytesAllocated_ << Endl;
        if (BytesAllocated_) {
            // WHY??
            Cerr << std::format("[MISHA] Bytes not freed: {}\n", BytesAllocated_);
            ReportFreeCallback_(BytesAllocated_);
        }
        // Y_ENSURE(BytesAllocated_ == 0, "Memory leak");
    }

private:
    ui64 BytesAllocated_{0};
    TReportCallback ReportAllocateCallback_{nullptr};
    TReportCallback ReportFreeCallback_{nullptr};
};

}//namespace NKikimr::NMiniKQL



