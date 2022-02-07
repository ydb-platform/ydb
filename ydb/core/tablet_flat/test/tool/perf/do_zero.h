#pragma once

#include "iface.h"

namespace NKikimr {
namespace NTable {
namespace NPerf {

    class TDoZero : public IPerfTable {
    public:
        TDoZero(ILogger*, const NTest::TMass&)
        {

        }

        void Seek(TRawVals, ESeek) override
        {

        }

        ui64 Scan(ui64 items, TSponge &aggr) override
        {
            for (ui64 it = 0; it < items; ++it)
                aggr(&it, sizeof(it));

            return 0;
        }

        void Once(TRawVals key, TSponge &aggr) override
        {
            Seek(key, ESeek::Exact);
            aggr(&Salt, sizeof(Salt));
            Salt++;
        }

    private:
        ui64 Salt = 0;
    };

}
}
}
