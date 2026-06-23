#pragma once

namespace NKikimr::NMiniKQL {

enum EFixedSizeScalarType {
    INT64,
    UUID,
    DECIMAL,
};

enum EFixedSizePackMode {
    SINGLE,
    BATCH,
};

} // namespace NKikimr::NMiniKQL
