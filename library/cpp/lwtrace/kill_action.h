#pragma once

#include "probe.h"

namespace NLWTrace {
    namespace NPrivate {
        class TKillActionExecutor: public IExecutor {
        public:
            explicit TKillActionExecutor(const TProbe*) {
            }
            bool DoExecute(TOrbit& orbit, const TParams& params) override;
        };

    }
}
