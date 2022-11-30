#pragma once

#include <util/system/types.h>

#include <util/generic/size_literals.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TStockpileOptions
{
    i64 BufferSize = 4_GB;
    int ThreadCount = 4;
    TDuration Period = TDuration::MilliSeconds(10);
};

void StockpileMemory(const TStockpileOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
