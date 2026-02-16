#include "request.h"

namespace NYdb::NBS::NBlockStore {

ui64 CreateRequestId()
{
    static std::atomic<ui64> RequestId(0);
    return RequestId.fetch_add(1);
}

}   // namespace NYdb::NBS::NBlockStore
