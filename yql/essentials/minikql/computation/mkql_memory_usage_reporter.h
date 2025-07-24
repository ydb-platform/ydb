#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <functional>
#include <format>

namespace NKikimr::NMiniKQL {

class TMemoryUsageReporter {
public:
    using TAllocateReportCallback = std::function<bool(ui64)>;
    using TFreeReportCallback = std::function<void(ui64)>;
    using TPtr = std::shared_ptr<TMemoryUsageReporter>;
public:
    TMemoryUsageReporter(TAllocateReportCallback reportAllocateCallback, TFreeReportCallback reportFreeCallback): ReportAllocateCallback_(reportAllocateCallback), ReportFreeCallback_(reportFreeCallback) {}
    void ReportAllocate(ui64 bytes) {
        // Cerr << "[MISHA][ALLOC]: " << bytes << ", Total: " << BytesAllocated_ << Endl;
        Y_ENSURE(ReportAllocateCallback_ != nullptr);
        if (ReportAllocateCallback_(bytes)) {
            BytesAllocated_ += bytes;
        }
    }

    void ReportFree(ui64 bytes) {
        // Cerr << "[MISHA][FREE]: " << bytes << ", Total: " << BytesAllocated_ << Endl;
        Y_ENSURE(ReportFreeCallback_ != nullptr);
        ui64 toFree = std::min(BytesAllocated_, bytes);
        if (toFree) {
            ReportFreeCallback_(bytes);
        }
        BytesAllocated_ -= toFree;
    }

    ~TMemoryUsageReporter() {
        // used only for test purposes. Must be changed to 
        // ReportFreeCallback_(BytesAllocated_);
        // Cerr << "[MISHA][DESTR]: " << "Total: " << BytesAllocated_ << Endl;
        if (BytesAllocated_) {
            // WHY??
            ReportFreeCallback_(BytesAllocated_);
        }
        Cerr << std::format("[MISHA] Bytes not freed: {}\n", BytesAllocated_);
        // Y_ENSURE(BytesAllocated_ == 0, "Memory leak");
    }

private:
    ui64 BytesAllocated_{0};
    TAllocateReportCallback ReportAllocateCallback_{nullptr};
    TFreeReportCallback ReportFreeCallback_{nullptr};
};

}//namespace NKikimr::NMiniKQL



