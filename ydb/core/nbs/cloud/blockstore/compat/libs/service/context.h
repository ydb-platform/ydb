#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/context.h>

namespace NCloud::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

using EProcessingStage = NYdb::NBS::EProcessingStage;
using TCallContextBase = NYdb::NBS::TCallContextBase;
using TCallContextBasePtr = NYdb::NBS::TCallContextBasePtr;
using TRequestTime = NYdb::NBS::TRequestTime;

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final: public TCallContextBase
{
private:
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

public:
    TCallContext(ui64 requestId = 0);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();
};

////////////////////////////////////////////////////////////////////////////////

inline TCallContextPtr CreateCallContext(ui64 requestId = 0)
{
    return MakeIntrusive<TCallContext>(requestId);
}

////////////////////////////////////////////////////////////////////////////////

TCallContextPtr ToBlockStoreCallContext(TCallContextBasePtr callContext);

}   // namespace NCloud::NBlockStore
