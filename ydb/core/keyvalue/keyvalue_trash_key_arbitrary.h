#pragma once
#include "defs.h"
#include <ydb/core/base/logoblob.h>

namespace NKikimr {
namespace NKeyValue {

#pragma pack(push, 1)
struct TTrashKeyArbitrary {
    TLogoBlobID LogoBlobId;
};
#pragma pack(pop)

} // NKeyValue
} // NKikimr
