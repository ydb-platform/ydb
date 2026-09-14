#pragma once

#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/generic/utility.h>

#define SHAREPLANNER_CHECK_EACH_ACTION

namespace NScheduling {

typedef ui64 TUCost;    // [res*sec]

// Different parrots for resource counting
typedef double TLength;   // [metre]
typedef double TForce;    // [newton]
typedef double TEnergy;   // [joule]
typedef double TDimless;  // [1]

typedef ui64 TWeight;
typedef double FWeight;

static const TForce gs = 1;

template <class W, class X>
X WCut(W w, W& wsum, X& xsum)
{
    Y_ASSERT(w > 0);
    Y_ASSERT(wsum > 0);
    Y_ASSERT(xsum > 0);
    X x = Max<X>(0, xsum * w / wsum);
    Y_ASSERT(x >= 0);
    wsum -= w;
    if (wsum < 0)
        wsum = 0;
    xsum -= x;
    if (xsum < 0)
        xsum = 0;
    return x;
}

template <class W, class X>
X WMaxCut(W w, X xmax, W& wsum, X& xsum)
{
    Y_ASSERT(w > 0);
    Y_ASSERT(wsum > 0);
    Y_ASSERT(xsum > 0);
    X x = Max<X>(0, Min<X>(xmax, xsum * w / wsum));
    Y_ASSERT(x >= 0);
    wsum -= w;
    if (wsum < 0)
        wsum = 0;
    xsum -= x;
    if (xsum < 0)
        xsum = 0;
    Y_ASSERT(wsum >= 0);
    Y_ASSERT(xsum >= 0);
    return x;
}

}
