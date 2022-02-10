#pragma once

#include "probe.h"

namespace NLWTrace {
    namespace NPrivate {
        class TSleepActionExecutor: public IExecutor {
        private:
            ui64 NanoSeconds;

        public:
            TSleepActionExecutor(const TProbe*, ui64 nanoSeconds)
                : IExecutor()
                , NanoSeconds(nanoSeconds)
            {
            }
            bool DoExecute(TOrbit& orbit, const TParams& params) override;
        };

    }
}
