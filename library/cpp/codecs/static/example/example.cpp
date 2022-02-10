#include "example.h"

#include <library/cpp/codecs/static/static.h> 

#include <util/generic/yexception.h>

extern "C" {
extern const ui8 codec_info_huff_20160707[];
extern const ui32 codec_info_huff_20160707Size;
extern const ui8 codec_info_sa_huff_20160707[];
extern const ui32 codec_info_sa_huff_20160707Size;
};

namespace NStaticCodecExample {
    static const NCodecs::TCodecConstPtr CODECS[] = {
        nullptr,
        NCodecs::RestoreCodecFromArchive(codec_info_huff_20160707, codec_info_huff_20160707Size),
        NCodecs::RestoreCodecFromArchive(codec_info_sa_huff_20160707, codec_info_sa_huff_20160707Size),
    };

    static_assert(Y_ARRAY_SIZE(CODECS) == DV_COUNT, "bad array size");

    void Encode(TBuffer& out, TStringBuf in, EDictVersion dv) {
        Y_ENSURE(dv > DV_NULL && dv < DV_COUNT, "invalid dict version: " << (int)dv);
        out.Clear();
        if (!in) {
            return;
        }
        CODECS[dv]->Encode(in, out);
        out.Append((char)dv);
    }

    void Decode(TBuffer& out, TStringBuf in) {
        out.Clear();
        if (!in) {
            return;
        }
        EDictVersion dv = (EDictVersion)in.back();
        Y_ENSURE(dv > DV_NULL && dv < DV_COUNT, "invalid dict version: " << (int)dv);
        in.Chop(1);
        CODECS[dv]->Decode(in, out);
    }
}
