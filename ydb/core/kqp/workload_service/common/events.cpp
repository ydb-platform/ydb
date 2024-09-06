#include "events.h"


namespace NKikimr::NKqp::NWorkload {

ui64 TPoolStateDescription::AmountRequests() const {
    return DelayedRequests + RunningRequests;
}

}  // NKikimr::NKqp::NWorkload
