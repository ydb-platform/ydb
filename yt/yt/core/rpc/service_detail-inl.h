#ifndef SERVICE_DETAIL_INL_H_
#error "Direct inclusion of this file is not allowed, include service_detail.h"
// For the sake of sane code completion.
#include "service_detail.h"
#endif

#include "helpers.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

template <class E>
void TServiceBase::DeclareServerFeature(E featureId)
{
    DoDeclareServerFeature(FeatureIdToInt(featureId));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
