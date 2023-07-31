#include "checker.h"

namespace NIPREG {

void TChecker::CheckNextFatal(const TAddress& first, const TAddress& last) {
    if (!CheckNext(first, last))
        ythrow yexception() << "IPREG format error: " << first.AsIPv6() << " - " << last.AsIPv6();
}

TFlatChecker::TFlatChecker() : HasState(false) {
}

bool TFlatChecker::CheckNext(const TAddress& first, const TAddress& last) {
    bool result = true;

    if (first > last)
        result = false;

    if (HasState && first <= PrevLast)
        result = false;

    PrevLast = last;
    HasState = true;

    return result;
}

TIntersectingChecker::TIntersectingChecker() : HasState(false) {
}

bool TIntersectingChecker::CheckNext(const TAddress& first, const TAddress& last) {
    bool result = true;

    if (first > last)
        result = false;

    if (HasState && (first < PrevFirst || (first == PrevFirst && last < PrevLast)))
        result = false;

    PrevFirst = first;
    PrevLast = last;
    HasState = true;

    return result;
}

}
