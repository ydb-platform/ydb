#include "scheme_borders.h"

#include <util/stream/trace.h>

namespace NKikimr {

    namespace {
        inline bool IsNullSuffix(const TConstArrayRef<TCell>& key, size_t start, size_t end) {
            for (size_t idx = start; idx < key.size() && Y_LIKELY(idx < end); ++idx) {
                if (key[idx]) {
                    return false; // there is a non-null column
                }
            }

            return true;
        }
    }

    int ComparePrefixBorders(
        TConstArrayRef<NScheme::TTypeInfo> keyTypes,
        TConstArrayRef<TCell> left, EPrefixMode leftMode,
        TConstArrayRef<TCell> right, EPrefixMode rightMode)
    {
        Y_DBGTRACE(VERBOSE, "ComparePrefixBorders(" << keyTypes.size() << " keys, "
            << left.size() << " left, mode=" << int(leftMode) << ", "
            << right.size() << " right, mode=" << int(rightMode) << ")");
        const size_t keySize = keyTypes.size();
        for (size_t idx = 0; idx < keySize; ++idx) {
            // Some key prefix up to idx is equal at this point
            bool leftPartial = (idx == left.size());
            bool rightPartial = (idx == right.size());

            if (leftPartial && rightPartial) {
                // both are extended at the same time
                int cmp = int(leftMode) - int(rightMode);
                Y_DBGTRACE(VERBOSE, "... both partial, cmp = " << cmp);
                return cmp;
            }

            if (leftPartial) {
                if (leftMode & PrefixModeFlagMax) {
                    // left extended with +inf > any other value
                    Y_DBGTRACE(VERBOSE, "... left partial, +inf > any => cmp = +1");
                    return +1;
                }
                if (!IsNullSuffix(right, idx, keySize)) {
                    // left extended with null < any other value
                    Y_DBGTRACE(VERBOSE, "... left partial, null < any => cmp = -1");
                    return -1;
                }
                if (right.size() >= keySize) {
                    // both keys are technically equal after extension
                    Y_DBGTRACE(VERBOSE, "... left partial, both technically null");
                    break;
                }
                // compare while taking extension mode into account
                int cmp = int(leftMode) - int(rightMode);
                Y_DBGTRACE(VERBOSE, "... left partial, cmp = " << cmp);
                return cmp;
            }

            if (rightPartial) {
                if (rightMode & PrefixModeFlagMax) {
                    // right extended with +inf > any other value
                    Y_DBGTRACE(VERBOSE, "... right partial, any < +inf => cmp = -1");
                    return -1;
                }
                if (!IsNullSuffix(left, idx, keySize)) {
                    // right extended with null < any other value
                    Y_DBGTRACE(VERBOSE, "... right partial, any > null => cmp = +1");
                    return +1;
                }
                if (left.size() >= keySize) {
                    // both keys are technically equal after extension
                    Y_DBGTRACE(VERBOSE, "... right partial, both technically null");
                    break;
                }
                // compare while taking extension mode into account
                int cmp = int(leftMode) - int(rightMode);
                Y_DBGTRACE(VERBOSE, "... right partial, cmp = " << cmp);
                return cmp;
            }

            if (int cmp = CompareTypedCells(left[idx], right[idx], keyTypes[idx])) {
                // use the true result of cell comparison
                Y_DBGTRACE(VERBOSE, "... cells not equal, cmp = " << cmp);
                return cmp;
            }
        }

        // left and right keys are equal, check relative ordering
        int cmp = (int(leftMode) & PrefixModeOrderMask) - (int(rightMode) & PrefixModeOrderMask);
        Y_DBGTRACE(VERBOSE, "... keys equal, cmp = " << cmp);
        return cmp;
    }

}
