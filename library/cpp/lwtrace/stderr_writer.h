#pragma once

#include "probe.h"

namespace NLWTrace {
    class TStderrActionExecutor: public IExecutor {
    private:
        TProbe* const Probe;

    public:
        explicit TStderrActionExecutor(TProbe* probe)
            : Probe(probe)
        {
        }

        bool DoExecute(TOrbit& orbit, const TParams& params) override;
    };

}
