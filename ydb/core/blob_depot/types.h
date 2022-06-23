#pragma once

#include "defs.h"

namespace NKikimr::NBlobDepot {

    static constexpr ui32 BaseDataChannel = 2;

    struct TChannelKind {
        std::array<ui8, 256> ChannelToIndex;
        std::vector<ui8> IndexToChannel;
    };

#pragma pack(push, 1)
    struct TVirtualGroupBlobFooter {
        TLogoBlobID StoredBlobId;
    };

#pragma pack(pop)

    static constexpr ui32 MaxBlobSize = 10 << 20; // 10 MB BlobStorage hard limit

    enum class EBlobType : ui32 {
        VG_COMPOSITE_BLOB = 0, // data + footer
        VG_DATA_BLOB = 1, // just data, footer aside
        VG_FOOTER_BLOB = 2, // footer only
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

        static TCGSI FromProto(const NKikimrBlobDepot::TBlobSeqId& proto) {
            return TCGSI{
                proto.GetChannel(),
                proto.GetGeneration(),
                proto.GetStep(),
                proto.GetIndex()
            };
        }

        TLogoBlobID MakeBlobId(ui64 tabletId, EBlobType type, ui32 part, ui32 size) const {
            return TLogoBlobID(tabletId, Generation, Step, Channel, size, MakeCookie(type, part));
        }

        ui32 MakeCookie(EBlobType type, ui32 part) const {
            switch (type) {
                case EBlobType::VG_COMPOSITE_BLOB:
                case EBlobType::VG_DATA_BLOB:
                case EBlobType::VG_FOOTER_BLOB:
                    static constexpr ui32 typeBits = 24 - IndexBits;
                    Y_VERIFY(static_cast<ui32>(type) < (1 << typeBits));
                    Y_VERIFY(!part);
                    return Index << typeBits | static_cast<ui32>(type);
            }

            Y_FAIL();
        }
    };

    enum class EKeepState : ui8 {
        Default,
        Keep,
        DoNotKeep
    };

} // NKikimr::NBlobDepot
