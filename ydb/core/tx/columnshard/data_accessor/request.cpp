#include "request.h"

namespace NKikimr::NOlap {

void IDataAccessorRequestsSubscriber::RegisterRequestId(const TDataAccessorsRequest& request) {
    AFL_VERIFY(RequestIds.emplace(request.GetRequestId()).second);
}

}   // namespace NKikimr::NOlap
