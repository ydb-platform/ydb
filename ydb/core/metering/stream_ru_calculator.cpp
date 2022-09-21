#include "stream_ru_calculator.h"

namespace NKikimr::NMetering {

TStreamRequestUnitsCalculator::TStreamRequestUnitsCalculator(ui64 blockSize)
    : BlockSize(blockSize)
    , Remainder(blockSize)
{
}

ui64 TStreamRequestUnitsCalculator::CalcConsumption(ui64 payloadSize) {
    if (!payloadSize) {
        return 0;
    }

    if (payloadSize > Remainder) {
        payloadSize -= Remainder;

        const ui64 nBlocks = payloadSize / BlockSize;
        payloadSize -= BlockSize * nBlocks;

        Remainder = BlockSize - payloadSize;
        return nBlocks + ui64(bool(payloadSize));
    } else {
        Remainder -= payloadSize;
        return 0;
    }
}

ui64 TStreamRequestUnitsCalculator::GetRemainder() const {
    return Remainder;
}

} // NKikimr::NMetering
