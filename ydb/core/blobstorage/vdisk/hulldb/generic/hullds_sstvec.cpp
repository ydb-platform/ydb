#include "hullds_sstvec_it.h"

namespace NKikimr {

    template struct TOrderedLevelSegments<TKeyLogoBlob, TMemRecLogoBlob>;
    template struct TOrderedLevelSegments<TKeyBarrier, TMemRecBarrier>;
    template struct TOrderedLevelSegments<TKeyBlock, TMemRecBlock>;

    template struct TUnorderedLevelSegments<TKeyLogoBlob, TMemRecLogoBlob>;
    template struct TUnorderedLevelSegments<TKeyBarrier, TMemRecBarrier>;
    template struct TUnorderedLevelSegments<TKeyBlock, TMemRecBlock>;

} // NKikimr
