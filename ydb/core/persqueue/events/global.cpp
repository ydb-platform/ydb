#include "global.h"
#include <ydb/core/persqueue/pqtablet/common/event_helpers.h>

bool NKikimr::TEvPersQueue::TEvProposeTransaction::GetSkipSrcIdInfo() const
{
    if (Record.GetTxBodyCase() != NKikimrPQ::TEvProposeTransaction::kData) {
        return false;
    }

    return NPQ::AllExistingWritesSkipConflictCheck(Record.GetData().GetOperations());
}
