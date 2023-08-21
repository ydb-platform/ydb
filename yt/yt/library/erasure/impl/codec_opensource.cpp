#include "codec.h"

#include "codec_detail.h"

#include <library/cpp/erasure/lrc_isa.h>
#include <library/cpp/erasure/reed_solomon_isa.h>

namespace NYT::NErasure {

using namespace ::NErasure;

////////////////////////////////////////////////////////////////////////////////

ICodec* FindCodec(ECodec codecId)
{
    // NB: Changing the set of supported codecs or their properties requires master reign promotion.
    switch (codecId) {
        // These codecs use ISA-l as a backend.
        case ECodec::ReedSolomon_3_3: {
            static NDetail::TCodec<TReedSolomonIsa<3, 3, 8, NDetail::TCodecTraits>> result(ECodec::ReedSolomon_3_3, /*bytewise*/ true);
            return &result;
        }
        case ECodec::IsaReedSolomon_6_3: {
            static NDetail::TCodec<TReedSolomonIsa<6, 3, 8, NDetail::TCodecTraits>> result(ECodec::IsaReedSolomon_6_3, /*bytewise*/ true);
            return &result;
        }
        case ECodec::IsaLrc_12_2_2: {
            static NDetail::TCodec<TLrcIsa<12, 4, 8, NDetail::TCodecTraits>> result(ECodec::IsaLrc_12_2_2, /*bytewise*/ true);
            return &result;
        }

        default:
            return nullptr;
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NErasure
