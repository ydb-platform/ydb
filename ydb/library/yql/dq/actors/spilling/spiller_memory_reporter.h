#pragma once

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <functional>
#include <memory>

namespace NYql::NDq {

class TSpillerMemoryUsageReporter {
public:
    using TReportAllocCallback = std::function<bool(ui64)>;
    using TReportFreeCallback = std::function<void(ui64)>;
    using TPtr = std::shared_ptr<TSpillerMemoryUsageReporter>;

public:
    TSpillerMemoryUsageReporter(TReportAllocCallback reportAllocCallback, TReportFreeCallback reportFreeCallback);
    ~TSpillerMemoryUsageReporter();

    void ReportAlloc(ui64 bytes);
    void ReportFree(ui64 bytes);

private:
    ui64 BytesAllocated_{0};
    TReportAllocCallback ReportAllocCallback_{nullptr};
    TReportFreeCallback ReportFreeCallback_{nullptr};
};

TSpillerMemoryUsageReporter::TPtr MakeSpillerMemoryUsageReporter(
    TSpillerMemoryUsageReporter::TReportAllocCallback reportAllocCallback,
    TSpillerMemoryUsageReporter::TReportFreeCallback reportFreeCallback);

} // namespace NYql::NDq
