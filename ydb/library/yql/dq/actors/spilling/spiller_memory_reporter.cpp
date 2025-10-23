#include "spiller_memory_reporter.h"

namespace NYql::NDq {

TSpillerMemoryUsageReporter::TSpillerMemoryUsageReporter(
    TMemoryReportCallback reportAllocCallback,
    TMemoryReportCallback reportFreeCallback)
    : ReportAllocCallback_(reportAllocCallback)
    , ReportFreeCallback_(reportFreeCallback)
{
    Y_ENSURE(reportAllocCallback != nullptr);
    Y_ENSURE(reportFreeCallback != nullptr);
}

void TSpillerMemoryUsageReporter::ReportAlloc(ui64 bytes) {
    ReportAllocCallback_(bytes);
    BytesAllocated_ += bytes;
}

void TSpillerMemoryUsageReporter::ReportFree(ui64 bytes) {
    // TODO(mfilitov): generate error if bytes > BytesAllocates_
    ui64 toFree = std::min(BytesAllocated_, bytes);
    if (toFree) {
        ReportFreeCallback_(toFree);
    }
    BytesAllocated_ -= toFree;
}

TSpillerMemoryUsageReporter::~TSpillerMemoryUsageReporter() {
    // TODO(mfilitov): generate error if BytesAllocated_ != 0
    // Probably this is legal when the request is cancelled
    if (BytesAllocated_) {
        ReportFreeCallback_(BytesAllocated_);
    }
}

TSpillerMemoryUsageReporter::TPtr MakeSpillerMemoryUsageReporter(
    TSpillerMemoryUsageReporter::TMemoryReportCallback reportAllocCallback,
    TSpillerMemoryUsageReporter::TMemoryReportCallback reportFreeCallback)
{
    if (reportAllocCallback == nullptr || reportFreeCallback == nullptr) {
        TSpillerMemoryUsageReporter::TMemoryReportCallback emptyCallback = [](ui64){};
        return std::make_shared<TSpillerMemoryUsageReporter>(emptyCallback, emptyCallback);
    }
    return std::make_shared<TSpillerMemoryUsageReporter>(reportAllocCallback, reportFreeCallback);
}

} // namespace NYql::NDq
