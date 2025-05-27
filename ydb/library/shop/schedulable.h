#pragma once

#include "resource.h"

#include <util/generic/ptr.h>

namespace NShop {

///////////////////////////////////////////////////////////////////////////////

template <class TCost>
struct TSchedulable: public virtual TThrRefBase {
public:
    TCost Cost;
};

}
