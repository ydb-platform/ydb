#pragma once

#include "mkql_spiller.h"

#include <ydb/library/yql/dq/actors/spilling/spilling_counters.h>

namespace NKikimr::NMiniKQL {

class ISpillerFactory : private TNonCopyable
{
public:
    virtual ISpiller::TPtr CreateSpiller() = 0;

    virtual void SetTaskCounters(TIntrusivePtr<NYql::NDq::TSpillingTaskCounters> spillingTaskCounters) = 0;

    virtual ~ISpillerFactory(){}
};

}//namespace NKikimr::NMiniKQL
