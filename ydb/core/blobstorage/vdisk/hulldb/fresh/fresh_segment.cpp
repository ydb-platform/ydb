#include "fresh_segment.h"

namespace NKikimr {

    template class TFreshIndexAndDataSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshIndexAndDataSnapshot<TKeyBarrier, TMemRecBarrier>;
    template class TFreshIndexAndDataSnapshot<TKeyBlock, TMemRecBlock>;

    template class TFreshIndexAndData<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshIndexAndData<TKeyBarrier, TMemRecBarrier>;
    template class TFreshIndexAndData<TKeyBlock, TMemRecBlock>;

    template class TFreshSegmentSnapshot<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshSegmentSnapshot<TKeyBarrier, TMemRecBarrier>;
    template class TFreshSegmentSnapshot<TKeyBlock, TMemRecBlock>;

    template class TFreshSegment<TKeyLogoBlob, TMemRecLogoBlob>;
    template class TFreshSegment<TKeyBarrier, TMemRecBarrier>;
    template class TFreshSegment<TKeyBlock, TMemRecBlock>;

} // NKikimr
