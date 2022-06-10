#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    static constexpr ui32 BaseDataChannel = 2;

    struct TChannelKind {
        std::array<ui8, 256> ChannelToIndex;
        std::vector<ui8> IndexToChannel;
    };

    struct TCGSI {
        static constexpr ui32 IndexBits = 20;
        static constexpr ui32 MaxIndex = (1 << IndexBits) - 1;

        ui32 Channel;
        ui32 Generation;
        ui32 Step;
        ui32 Index;

        ui64 ToBinary(const TChannelKind& kind) const {
            Y_VERIFY_DEBUG(Index <= MaxIndex);
            Y_VERIFY(Channel < kind.ChannelToIndex.size());
            return (static_cast<ui64>(Step) << IndexBits | Index) * kind.IndexToChannel.size() + kind.ChannelToIndex[Channel];
        }

        static TCGSI FromBinary(ui32 generation, const TChannelKind& kind, ui64 value) {
            static_assert(sizeof(long long) >= sizeof(ui64));
            auto res = std::lldiv(value, kind.IndexToChannel.size());

            return TCGSI{
                .Channel = kind.IndexToChannel[res.rem],
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
