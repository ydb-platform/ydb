#include "events.h"


namespace NKikimr::NWorkloadManager {

ui64 TPoolStateDescription::AmountRequests() const {
    return DelayedRequests + RunningRequests;
}

}  // NKikimr::NWorkloadManager
