#include "resource_vector.h"

namespace NYT::NVectorHdrf {

////////////////////////////////////////////////////////////////////////////////
    
TResourceVector TResourceVector::FromJobResources(
    const TJobResources& resources,
    const TJobResources& totalLimits,
    double zeroDivByZero,
    double oneDivByZero)
{
    auto computeResult = [&] (auto resourceValue, auto resourceLimit, double& result) {
        if (static_cast<double>(resourceLimit) == 0.0) {
            if (static_cast<double>(resourceValue) == 0.0) {
                result = zeroDivByZero;
            } else {
                result = oneDivByZero;
            }
        } else {
            result = static_cast<double>(resourceValue) / static_cast<double>(resourceLimit);
        }
    };

    TResourceVector resultVector;
    #define XX(name, Name) computeResult(resources.Get##Name(), totalLimits.Get##Name(), resultVector[EJobResourceType::Name]);
    ITERATE_JOB_RESOURCES(XX)
    #undef XX
    return resultVector;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NVectorHdrf

