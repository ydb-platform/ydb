#include "fresh_datasnap.h"

namespace NKikimr {

    template class TFreshDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshDataSnapshot<TKeyBarrier, TMemRecBarrier>;
    template class TFreshDataSnapshot<TKeyBlock, TMemRecBlock>;

} // NKikimr
