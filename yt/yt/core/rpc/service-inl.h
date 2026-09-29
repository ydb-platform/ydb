#ifndef SERVICE_INL_H_
#error "Direct inclusion of this file is not allowed, include service.h"
// For the sake of sane code completion.
#include "service.h"
#endif

#include "helpers.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

namespace NDetail {

bool IsClientFeatureSupported(const IServiceContext* context, int featureId);
void ThrowUnsupportedClientFeature(int featureId, TStringBuf featureName);

} // namespace NDetail

inline auto IServiceContext::AnnotateRequest(bool flush)
{
    return NLogging::TLoggingTagListBuilderGuard(
        GetRequestAnnotations(),
        [this, flush] {
            CommitRequestAnnotations(flush);
        });
}

inline auto IServiceContext::AnnotateResponse()
{
    return NLogging::TLoggingTagListBuilderGuard(GetResponseAnnotations());
}

template <class E>
bool IServiceContext::IsClientFeatureSupported(E featureId) const
{
    return NDetail::IsClientFeatureSupported(this, FeatureIdToInt(featureId));
}

template <class E>
void IServiceContext::ValidateClientFeature(E featureId) const
{
    auto intFeatureId = FeatureIdToInt(featureId);
    if (!NDetail::IsClientFeatureSupported(this, intFeatureId)) {
        NDetail::ThrowUnsupportedClientFeature(intFeatureId, TEnumTraits<E>::ToString(featureId));
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
