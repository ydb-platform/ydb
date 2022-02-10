#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

struct TRequestType {
    enum EType {
        ReadOnly = 0,
        WriteOnly = 1,
        ReadWrite = 2,
        ReadOnlyInline = 3
    };
};

} // NKeyValue
} // NKikimr
