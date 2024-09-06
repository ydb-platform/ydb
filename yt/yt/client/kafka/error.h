#pragma once

#include "public.h"

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EErrorCode, i16,
    ((UnknownServerError)           (-1))
    ((None)                         (0))
    ((TopicAuthorizationFailed)     (29))
    ((GroupAuthorizationFailed)     (30))
    ((SaslAuthenticationFailed)     (31))
    ((UnsupportedSaslMechanism)     (33))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
