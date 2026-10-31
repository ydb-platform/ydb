#include "request.h"

namespace NYdb::NBS::NGrpc {

////////////////////////////////////////////////////////////////////////////////

void* TRequestHandlerBase::AcquireCompletionTag()
{
    ++RefCount;
    return this;
}

void TRequestHandlerBase::ReleaseCompletionTag()
{
    if (--RefCount == 0) {
        delete this;
    }
}

}   // namespace NYdb::NBS::NGrpc
