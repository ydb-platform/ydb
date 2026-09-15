#ifndef VERSION_INL_H_
#error "Direct inclusion of this file is not allowed, include version.h"
// For the sake of sane code completion.
#include "version.h"
#endif

namespace NYT::NHydra {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
[[nodiscard]] TImpl TExtendedVersionBase<TImpl>::Advance(int delta) const
{
    YT_VERIFY(delta > 0);

    return TImpl(SegmentId, RecordId + delta);
}

template <class TImpl>
[[nodiscard]] TImpl TExtendedVersionBase<TImpl>::Rotate() const
{
    return TImpl(SegmentId + 1, 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NHydra
