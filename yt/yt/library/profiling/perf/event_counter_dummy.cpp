#include "event_counter.h"

namespace NYT::NProfiling {

////////////////////////////////////////////////////////////////////////////////

class TPerfEventCounter
    : public IPerfEventCounter
{
public:
    virtual i64 Read() final
    {
        return 0;
    }
};

std::unique_ptr<IPerfEventCounter> CreatePerfEventCounter(EPerfEventType /*type*/)
{
    return std::make_unique<TPerfEventCounter>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NProfiling
