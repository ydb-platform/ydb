#pragma once

#include "sponge.h"
#include "logger.h"

#include <ydb/core/tablet_flat/defs.h>
#include <ydb/core/tablet_flat/test/libs/rows/mass.h>

namespace NKikimr {
namespace NTable {
namespace NPerf {

    class IPerfTable {
    public:
        using TSponge = NTest::TSponge;
        using TMass = NTest::TMass;

        virtual ~IPerfTable() = default;

        virtual void Seek(TRawVals key, ESeek seek) = 0;
        virtual ui64 Scan(ui64 items, TSponge &aggr) = 0;
        virtual void Once(TRawVals key, TSponge &aggr) = 0;
    };

}
}
}
