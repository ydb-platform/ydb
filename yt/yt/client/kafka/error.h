#pragma once

#include "public.h"

namespace NYT::NKafka {

////////////////////////////////////////////////////////////////////////////////

DEFINE_ENUM_WITH_UNDERLYING_TYPE(EErrorCode, i16,
    ((UnknownServerError)           (-1))
    ((None)                         (0))
    ((NotCoordinator)               (16))
    ((IllegalGeneration)            (22))
    ((InconsistentGroupProtocol)    (23))
    ((RebalanceInProgress)          (27))
    ((TopicAuthorizationFailed)     (29))
    ((GroupAuthorizationFailed)     (30))
    ((SaslAuthenticationFailed)     (31))
    ((InvalidTimestamp)             (32))
    ((UnsupportedSaslMechanism)     (33))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NKafka
