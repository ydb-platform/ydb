#pragma once

#include "public.h"

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

//! The static properties of a codec, usable without depending on erasure/impl.
struct TCodecParams
{
    int DataPartCount = 0;
    int ParityPartCount = 0;
    int TotalPartCount = 0;

    //! The maximum number of parts that can always be repaired when missing.
    int GuaranteedRepairablePartCount = 0;

    //! Every block passed to the codec must have size divisible by this.
    int WordSize = 0;

    //! Whether the i-th byte of any parity part depends only on the i-th bytes of the data parts.
    bool Bytewise = false;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
