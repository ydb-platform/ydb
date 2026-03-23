#include "global.h"
#include "internal.h"

bool NKikimr::TEvPersQueue::TEvProposeTransaction::GetSkipSrcIdInfo() const
{
    if (Record.GetTxBodyCase() != NKikimrPQ::TEvProposeTransaction::kData) {
        return false;
    }

    const auto& tx = Record.GetData();
    for (const auto& op : tx.GetOperations()) {
        if (op.GetSkipConflictCheck()) {
            return true;
        }
    }

    return false;
}

bool NKikimr::TEvPQ::TEvTxCalcPredicate::GetSkipSrcIdInfo() const
{
    for (const auto& op : Operations) {
        if (op.GetSkipConflictCheck()) {
            return true;
        }
    }

    return false;
}
