#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    static constexpr ui32 BaseDataChannel = 2;

    struct TCGSI {
        static constexpr ui32 IndexBits = 20;
        static constexpr ui32 MaxIndex = (1 << IndexBits) - 1;

        ui32 Channel;
        ui32 Generation;
        ui32 Step;
        ui32 Index;

        ui64 ToBinary(ui32 numChannels) const {
            Y_VERIFY_DEBUG(numChannels > BaseDataChannel);
            Y_VERIFY_DEBUG(Index <= MaxIndex);
            return (static_cast<ui64>(Step) << IndexBits | Index) * (numChannels - BaseDataChannel) + (Channel - BaseDataChannel);
        }

        static TCGSI FromBinary(ui32 generation, ui32 numChannels, ui64 value) {
            static_assert(sizeof(long long) >= sizeof(ui64));
            auto res = std::lldiv(value, numChannels - BaseDataChannel);

            return TCGSI{
                .Channel = static_cast<ui32>(res.rem + BaseDataChannel),
                .Generation = generation,
                .Step = static_cast<ui32>(res.quot >> IndexBits),
                .Index = static_cast<ui32>(res.quot) & MaxIndex
            };
        }
    };

    enum class EKeepState : ui8 {
        Default,
        Keep,
        DoNotKeep
    };

} // NKikimr::NBlobDepot
