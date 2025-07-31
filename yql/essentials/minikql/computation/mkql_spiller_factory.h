#pragma once

#include "mkql_spiller.h"

namespace NYql::NDq {
struct TSpillingTaskCounters;
}

namespace NKikimr::NMiniKQL {

class ISpillerFactory : private TNonCopyable
{
public:
    virtual ISpiller::TPtr CreateSpiller() = 0;

    virtual void SetTaskCounters(const TIntrusivePtr<NYql::NDq::TSpillingTaskCounters>& spillingTaskCounters) = 0;

    virtual void SetMemoryReportingCallbacks(std::function<bool(ui64)> reportAlloc, std::function<void(ui64)> reportFree) = 0;

    virtual ~ISpillerFactory(){}
};

}//namespace NKikimr::NMiniKQL
