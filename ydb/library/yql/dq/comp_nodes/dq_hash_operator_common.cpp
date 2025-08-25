#include "dq_hash_operator_common.h"

namespace NKikimr {
namespace NMiniKQL {
namespace NDqHashOperatorCommon {

TStatKey DqHashCombine_FlushesCount("DqHashCombine_FlushesCount", true);
TStatKey DqHashCombine_MaxRowsCount("DqHashCombine_MaxRowsCount", false);

}
}
}