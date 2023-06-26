#include "erasure.h"

namespace NKikimr {

    void ErasureRestore(TErasureType::ECrcMode crcMode, TErasureType erasure, ui32 fullSize, TRope *whole,
            std::span<TRope> parts, ui32 restoreMask) {
        Y_VERIFY(parts.size() == erasure.TotalPartCount());

        TDataPartSet p;
        p.FullDataSize = fullSize;
        for (ui32 i = 0; i < parts.size(); ++i) {
            TPartFragment fragment;
            fragment.ResetToWhole(parts[i]);
            p.Parts.push_back(std::move(fragment));
            if (parts[i]) {
                p.PartsMask |= 1 << i;
            }
        }

        TRope temp;
        erasure.RestoreData(crcMode, p, whole ? *whole : temp, restoreMask & ((1 << erasure.DataParts()) - 1),
            whole != nullptr, restoreMask >> erasure.DataParts());

        for (ui32 i = 0; i < parts.size(); ++i) {
            parts[i] = std::move(p.Parts[i].OwnedString);
        }
    }

} // NKikimr
