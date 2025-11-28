#pragma once

#include "public.h"

#include <util/datetime/base.h>

namespace NYT::NTransactionClient {

////////////////////////////////////////////////////////////////////////////////

ITimestampProviderPtr CreateBatchingTimestampProvider(
    ITimestampProviderPtr underlying,
    const TRemoteTimestampProviderConfigPtr& config);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NTransactionClient
