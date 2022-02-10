#pragma once

#include "common.h"
#include "uri.h"
#include <string>

namespace NUri {
    class TQueryArgProcessing {
    public:
        TQueryArgProcessing(ui32 flags, TQueryArgFilter filter = 0, void* filterData = 0);

        TQueryArg::EProcessed Process(TUri& uri);

    private:
        ui32 Flags;
        TQueryArgFilter Filter;
        void* FilterData;

        class Pipeline;
        std::string Buffer;
    };
}
