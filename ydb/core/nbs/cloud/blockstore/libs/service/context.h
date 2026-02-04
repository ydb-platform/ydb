#pragma once

#include "public.h"

#include <ydb/core/nbs/cloud/storage/core/libs/common/context.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct TCallContext final
    : public TCallContextBase
{
private:
    TAtomic SilenceRetriableErrors = false;
    TAtomic HasUncountableRejects = false;

public:
    explicit TCallContext(ui64 requestId = 0);

    bool GetSilenceRetriableErrors() const;
    void SetSilenceRetriableErrors(bool silence);

    bool GetHasUncountableRejects() const;
    void SetHasUncountableRejects();
};

}   // namespace NYdb::NBS::NBlockStore
