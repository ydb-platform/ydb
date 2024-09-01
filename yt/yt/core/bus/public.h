#pragma once

#include <yt/yt/core/misc/public.h>

#include <util/generic/size_literals.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

DECLARE_REFCOUNTED_STRUCT(IBus)
DECLARE_REFCOUNTED_STRUCT(IMessageHandler)
DECLARE_REFCOUNTED_STRUCT(IBusClient)
DECLARE_REFCOUNTED_STRUCT(IBusServer)

struct TBusNetworkStatistics;

using TTosLevel = int;
constexpr int DefaultTosLevel = 0;
constexpr int BlackHoleTosLevel = -1;

constexpr size_t MaxMessagePartCount = 1 << 28;
constexpr size_t MaxMessagePartSize = 1_GB;

DEFINE_ENUM(EDeliveryTrackingLevel,
    (None)
    (ErrorOnly)
    (Full)
);

DEFINE_ENUM(EMultiplexingBand,
    ((Default)               (0))
    ((Control)               (1))
    ((Heavy)                 (2))
    ((Interactive)           (3))
    ((RealTime)              (4))
);

YT_DEFINE_ERROR_ENUM(
    ((TransportError)       (100))
    ((SslError)             (119))
);

DEFINE_ENUM(EEncryptionMode,
    ((Disabled)     (0))
    ((Optional)     (1))
    ((Required)     (2))
);

DEFINE_ENUM(EVerificationMode,
    ((None)     (0))    // Do no certificate or host name verifications.
    ((Ca)       (1))    // Verifies peer's certificate with the CA.
    ((Full)     (2))    // Verifies peer's certificate with the CA as well as peer's host name against the certificate.
);

extern const TString DefaultNetworkName;
extern const TString LocalNetworkName;

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

