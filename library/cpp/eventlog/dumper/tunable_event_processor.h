#pragma once

#include <library/cpp/eventlog/eventlog.h>

namespace NLastGetopt {
    class TOpts;
}

class ITunableEventProcessor: public IEventProcessor {
public:
    virtual void SetEventProcessor(IEventProcessor* /*processor*/) {
    }

    virtual void AddOptions(NLastGetopt::TOpts& opts) = 0;
    virtual void CheckOptions() {
    }
    virtual ~ITunableEventProcessor() {
    }
};
