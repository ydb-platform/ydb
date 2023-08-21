#pragma once

#include <library/cpp/yt/misc/enum.h>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

DEFINE_AMBIGUOUS_ENUM_WITH_UNDERLYING_TYPE(ECodec, i8,
    ((None)                         (0))

    ((ReedSolomon_6_3)              (1))
    ((JerasureReedSolomon_6_3)      (1))
    ((IsaReedSolomon_6_3)           (5))

    ((ReedSolomon_3_3)              (4))
    ((IsaReedSolomon_3_3)           (4))

    ((Lrc_12_2_2)                   (2))
    ((JerasureLrc_12_2_2)           (2))
    ((IsaLrc_12_2_2)                (3))
);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
