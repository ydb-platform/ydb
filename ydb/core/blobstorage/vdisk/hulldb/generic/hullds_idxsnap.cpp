#include "hullds_idxsnap_it.h"

namespace NKikimr {

    template class TLevelIndexSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TLevelIndexSnapshot<TKeyBarrier, TMemRecBarrier>;
    template class TLevelIndexSnapshot<TKeyBlock, TMemRecBlock>;

} // NKikimr
