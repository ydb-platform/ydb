#pragma once

#include <yql/essentials/minikql/computation/mkql_spiller.h>

#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <memory>

namespace NYql::NDq {

class TSpillerMemoryUsageReporter {
public:
    using TPtr = std::shared_ptr<TSpillerMemoryUsageReporter>;
    using TMemoryReportCallback = NKikimr::NMiniKQL::ISpiller::TMemoryReportCallback;

public:
    TSpillerMemoryUsageReporter(TMemoryReportCallback reportAllocCallback, TMemoryReportCallback reportFreeCallback);
    ~TSpillerMemoryUsageReporter();

    void ReportAlloc(ui64 bytes);
    void ReportFree(ui64 bytes);

private:
    ui64 BytesAllocated_{0};
    TMemoryReportCallback ReportAllocCallback_{nullptr};
    TMemoryReportCallback ReportFreeCallback_{nullptr};
};

TSpillerMemoryUsageReporter::TPtr MakeSpillerMemoryUsageReporter(
    TSpillerMemoryUsageReporter::TMemoryReportCallback reportAllocCallback,
    TSpillerMemoryUsageReporter::TMemoryReportCallback reportFreeCallback);

} // namespace NYql::NDq
