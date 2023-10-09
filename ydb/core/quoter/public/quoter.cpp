#include "quoter.h"

namespace NKikimr {

NActors::TActorId MakeQuoterServiceID() {
    char x[12] = { 'q', 'u', 'o', 't', 'e', 'r', 's', 'v', 'c' };
    return NActors::TActorId(0, TStringBuf(x, 12));
}

ui64 TEvQuota::TResourceLeaf::MakeTaggedRateRes(ui32 tag, ui32 rate) {
    Y_ABORT_UNLESS(rate <= 0x3FFFFFFF);
    return (1ULL << 62) | (static_cast<ui64>(tag) << 30) | (static_cast<ui64>(rate) & 0x3FFFFFFF);
}

}
