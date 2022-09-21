#pragma once

#include <util/generic/fwd.h>

namespace NKikimr::NMetering {

class TStreamRequestUnitsCalculator {
public:
    // Remainder = blockSize on init
    explicit TStreamRequestUnitsCalculator(ui64 blockSize);

    // Returns consumption in terms of RUs (one block is one RU)  and updates remainder
    ui64 CalcConsumption(ui64 payloadSize);

    ui64 GetRemainder() const;

private:
    const ui64 BlockSize;
    ui64 Remainder; // remainder in the last block

};

} // NKikimr::NMetering
