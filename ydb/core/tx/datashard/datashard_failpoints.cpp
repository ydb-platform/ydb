#include "datashard_failpoints.h"

namespace NKikimr {
namespace NDataShard {

TCancelTxFailPoint gCancelTxFailPoint;
TSkipRepliesFailPoint gSkipRepliesFailPoint;
TSkipReadIteratorResultFailPoint gSkipReadIteratorResultFailPoint;

} // namespace NDataShard
} // namespace NKikimr
