#pragma once

#include "defs.h"

namespace NKikimr {
namespace NTabletFlatExecutor {

    /**
     * Interface for processing individual blob fetch results
     */
    class ILoadBlob {
    public:
        virtual ~ILoadBlob() = default;

        virtual void OnBlobLoaded(const TLogoBlobID& id, TString body, uintptr_t cookie) = 0;
    };

}
}
