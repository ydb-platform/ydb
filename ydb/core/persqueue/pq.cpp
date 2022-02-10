#include "pq_impl.h"
#include "read_balancer.h"

namespace NKikimr {

IActor* CreatePersQueue(const TActorId& tablet, TTabletStorageInfo *info) {
    return new NPQ::TPersQueue(tablet, info);
}

IActor* CreatePersQueueReadBalancer(const TActorId& tablet, TTabletStorageInfo *info) {
    return new NPQ::TPersQueueReadBalancer(tablet, info);
}


} // NKikimr
