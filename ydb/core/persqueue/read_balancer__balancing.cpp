#include "read_balancer.h"


namespace NKikimr::NPQ {

//
// TPartitionFamilty
//

TPersQueueReadBalancer::TPartitionFamilty::TPartitionFamilty()
    : ActivePartitionCount(0)
    , InactivePartitionCount(0)
{}



//
// TBalancingConsumerInfo
//

TPersQueueReadBalancer::TBalancingConsumerInfo::TBalancingConsumerInfo()
    : NextFamilyId(0)
{}

}
