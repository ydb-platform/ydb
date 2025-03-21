#pragma once

#include "flat_abi_evol.h"
#include "util_fmt_abort.h"

namespace NKikimr {
namespace NTable {

    struct TAbi {
        using EVol = ECompatibility;

        void Check(ui32 tail, ui32 head, const char *label) const
        {
            if (tail > head) {
                Y_TABLET_ERROR(
                    label << " ABI [" << tail << ", " << head << "]"
                    << " label is invalid");
            } else if (head < ui32(EVol::Tail) || tail > ui32(EVol::Edge)) {
                Y_TABLET_ERROR(
                    "NTable read ABI [" << ui32(EVol::Tail) << ", "
                    << ui32(EVol::Edge) << "] is incompatible with ABI"
                    << " [" << tail << ", " << head << "] of " << label
                );
            }
        }
    };

}
}
