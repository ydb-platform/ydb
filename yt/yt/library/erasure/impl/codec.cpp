#include "codec.h"

#include "codec_detail.h"

#include <yt/yt/core/misc/error.h>

namespace NYT::NErasure {

////////////////////////////////////////////////////////////////////////////////

int ICodec::GetTotalPartCount() const
{
    return GetDataPartCount() + GetParityPartCount();
}

////////////////////////////////////////////////////////////////////////////////

ICodec* GetCodec(ECodec id)
{
    if (auto* codec = FindCodec(id)) {
        return codec;
    }

    THROW_ERROR_EXCEPTION("Unsupported erasure codec %Qlv", id);
}

ECodec GetEffectiveCodecId(ECodec id)
{
    ECodec effectiveCodecId;
    switch (id) {
        case ECodec::JerasureReedSolomon_6_3:
        case ECodec::IsaReedSolomon_6_3:
            effectiveCodecId = ECodec::IsaReedSolomon_6_3;
            break;

        case ECodec::IsaReedSolomon_3_3:
            effectiveCodecId = ECodec::IsaReedSolomon_3_3;
            break;

        case ECodec::JerasureLrc_12_2_2:
        case ECodec::IsaLrc_12_2_2:
            effectiveCodecId = ECodec::IsaLrc_12_2_2;
            break;

        case ECodec::None:
            effectiveCodecId = ECodec::None;
            break;

        default:
            YT_ABORT();
    }

    if (FindCodec(effectiveCodecId)) {
        return effectiveCodecId;
    }

    return id;
}

const std::vector<ECodec>& GetSupportedCodecIds()
{
    static const std::vector<ECodec> supportedCodecIds = [] {
        std::vector<ECodec> codecIds;
        for (auto codecId : TEnumTraits<ECodec>::GetDomainValues()) {
            if (FindCodec(codecId)) {
                codecIds.push_back(codecId);
            }
        }
        codecIds.push_back(ECodec::None);

        SortUnique(codecIds);

        return codecIds;
    }();
    return supportedCodecIds;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NChunkClient

