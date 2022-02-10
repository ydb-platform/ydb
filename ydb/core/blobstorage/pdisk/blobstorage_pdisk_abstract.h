#pragma once
#include "defs.h"
#include <util/generic/ptr.h>

namespace NKikimr {
namespace NPDisk {

class IPDisk : public TThrRefBase {
public:
    virtual void Update() = 0;
    virtual void Wakeup() = 0;
    virtual ~IPDisk() = default;
};

} // NPDisk
} // NKikimr
