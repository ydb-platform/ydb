#include "spiller_memory_reporter.h"

#include <format>

namespace NYql::NDq {

TSpillerMemoryUsageReporter::TSpillerMemoryUsageReporter(
    TReportAllocCallback reportAllocCallback,
    TReportFreeCallback reportFreeCallback)
    : ReportAllocCallback_(reportAllocCallback)
    , ReportFreeCallback_(reportFreeCallback)
{}

void TSpillerMemoryUsageReporter::ReportAlloc(ui64 bytes) {
    Y_ENSURE(ReportAllocCallback_ != nullptr);
    if (ReportAllocCallback_(bytes)) {
        BytesAllocated_ += bytes;
    }
}

void TSpillerMemoryUsageReporter::ReportFree(ui64 bytes) {
    Y_ENSURE(ReportFreeCallback_ != nullptr);
    ui64 toFree = std::min(BytesAllocated_, bytes);
    if (toFree) {
        ReportFreeCallback_(bytes);
    }
    BytesAllocated_ -= toFree;
}

TSpillerMemoryUsageReporter::~TSpillerMemoryUsageReporter() {
    if (BytesAllocated_) {
        ReportFreeCallback_(BytesAllocated_);
    }
    Cerr << std::format("[MISHA] Bytes not freed: {}\n", BytesAllocated_);
    // Y_ENSURE(BytesAllocated_ == 0, "Memory leak");
}

TSpillerMemoryUsageReporter::TPtr MakeSpillerMemoryUsageReporter(
    TSpillerMemoryUsageReporter::TReportAllocCallback reportAllocCallback,
    TSpillerMemoryUsageReporter::TReportFreeCallback reportFreeCallback)
{
    return std::make_shared<TSpillerMemoryUsageReporter>(reportAllocCallback, reportFreeCallback);
}

} // namespace NYql::NDq

