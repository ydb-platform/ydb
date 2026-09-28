#pragma once

#include "public.h"

#include <yt/yt/library/erasure/public.h>

namespace NYT::NJournalClient {

////////////////////////////////////////////////////////////////////////////////

void ValidateReplicatedJournalAttributes(
    int replicationFactor,
    int readQuorum,
    int writeQuorum);

void ValidateErasureJournalAttributes(
    NErasure::ECodec codecId,
    const NErasure::TCodecParams& codecParams,
    int replicationFactor,
    int readQuorum,
    int writeQuorum);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NJournalClient
