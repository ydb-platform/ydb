#pragma once

#include <util/system/types.h>

#include <util/generic/size_literals.h>

#include <util/datetime/base.h>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

struct TStockpileOptions
{
    static constexpr i64 DefaultBufferSize = 4_GBs;
    i64 BufferSize = DefaultBufferSize;

    static constexpr int DefaultThreadCount = 4;
    int ThreadCount = DefaultThreadCount;

    static constexpr TDuration DefaultPeriod = TDuration::MilliSeconds(10);
    TDuration Period = DefaultPeriod;
};

void ConfigureStockpile(const TStockpileOptions& options);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
