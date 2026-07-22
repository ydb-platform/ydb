#pragma once

#include "public.h"

#include <ydb/library/actors/wilson/wilson_span.h>

#include <util/system/types.h>

namespace NYdb::NBS::NBlockStore {

////////////////////////////////////////////////////////////////////////////////

struct ITraceService
{
    virtual ~ITraceService() = default;

    [[nodiscard]] virtual NWilson::TSpan CreteRootSpan(TStringBuf name) = 0;
};

////////////////////////////////////////////////////////////////////////////////

}   // namespace NYdb::NBS::NBlockStore
