#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::HandleS3Result(TAutoPtr<IEventHandle>) { Y_ABORT(); }

    void TBlobDepotAgent::HandleS3GetThrottleWakeup() {}

}
