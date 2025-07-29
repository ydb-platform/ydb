#pragma once

#include "mkql_spiller.h"
#include "mkql_memory_usage_reporter.h"

namespace NYql::NDq {
struct TSpillingTaskCounters;
}

namespace NKikimr::NMiniKQL {

class ISpillerFactory : private TNonCopyable
{
public:
    virtual ISpiller::TPtr CreateSpiller() = 0;

    virtual void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& spillingTaskCounters) = 0;

    virtual void SetMemoryUsageReporter(TMemoryUsageReporter::TPtr memoryUsageReporter) = 0;
    virtual TMemoryUsageReporter::TPtr GetMemoryUsageReporter() const = 0;

    virtual ~ISpillerFactory(){}
};

}//namespace NKikimr::NMiniKQL
