#pragma once
#include "defs.h"

namespace NKikimr {
namespace NKeyValue {

enum EItemType {
    EIT_UNKNOWN = 0,
    EIT_BEGIN = 1,
    EIT_KEYVALUE_1 = 2,  // For backward copmatibility only.
    EIT_TRASH = 3,
    EIT_COLLECT = 4,
    EIT_STATE = 5,
    EIT_KEYVALUE_2 = 6,
    EIT_VACUUM_GENERATION = 7,
    EIT_END = 8
};

} // NKeyValue
} // NKikimr
