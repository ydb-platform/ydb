#include "request_discriminator.h"

namespace NKikimr::NJaegerTracing {

const TRequestDiscriminator TRequestDiscriminator::EMPTY {
    .RequestType = ERequestType::UNSPECIFIED,
};

} // namespace NKikimr::NJaegerTracing
