#include "global.h"
#include "internal.h"

template <class C>
bool AllExistingWritesSkipConflictCheck(const C& ops)
{
    size_t writeOpsCount = 0;
    size_t flagsCount = 0;

    for (const auto& op : ops) {
        if (!op.HasSkipConflictCheck()) {
            // операция чтения
            continue;
        }

        ++writeOpsCount;

        if (op.GetSkipConflictCheck()) {
            ++flagsCount;
        }
    }

    return (writeOpsCount > 0) && (writeOpsCount == flagsCount);
}

bool NKikimr::TEvPersQueue::TEvProposeTransaction::GetSkipSrcIdInfo() const
{
    if (Record.GetTxBodyCase() != NKikimrPQ::TEvProposeTransaction::kData) {
        return false;
    }

    return AllExistingWritesSkipConflictCheck(Record.GetData().GetOperations());
}

bool NKikimr::TEvPQ::TEvTxCalcPredicate::GetSkipSrcIdInfo() const
{
    return AllExistingWritesSkipConflictCheck(Operations);
}
