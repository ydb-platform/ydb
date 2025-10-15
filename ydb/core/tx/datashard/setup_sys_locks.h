#pragma once
#include "defs.h"
#include "operation.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

struct TSetupSysLocks
    : public TLocksUpdate
{
    TSysLocks &SysLocksTable;

    TSetupSysLocks(TDataShard& self, ILocksDb* db)
        : SysLocksTable(self.SysLocksTable())
    {
        SysLocksTable.SetupUpdate(this, db);
    }

    TSetupSysLocks(ui64 lockTxId, ui32 lockNodeId, TDataShard& self, ILocksDb* db)
        : SysLocksTable(self.SysLocksTable())
    {
        LockTxId = lockTxId;
        LockNodeId = lockNodeId;

        SysLocksTable.SetupUpdate(this, db);
    }

    TSetupSysLocks(TOperation::TPtr op,
                   TDataShard &self,
                   ILocksDb* db)
        : SysLocksTable(self.SysLocksTable())
    {
        LockTxId = op->LockTxId();
        LockNodeId = op->LockNodeId();

        auto mvccVersion = self.GetMvccVersion(op.Get());

        // check whether the current operation is a part of an out-of-order Tx
        bool outOfOrder = false;
        if (auto &activeOps = self.Pipeline.GetActivePlannedOps()) {
            if (auto it = activeOps.begin(); mvccVersion != TRowVersion(it->first.Step, it->first.TxId))
                outOfOrder = true;
        }

        CheckVersion = mvccVersion;
        BreakVersion = outOfOrder ? mvccVersion : TRowVersion::Min();

        if (!op->LocksCache().Locks.empty())
            SysLocksTable.SetCache(&op->LocksCache());
        else
            SysLocksTable.SetAccessLog(&op->LocksAccessLog());
        SysLocksTable.SetupUpdate(this, db);
    }

    ~TSetupSysLocks() {
        SysLocksTable.ResetUpdate();
        SysLocksTable.SetCache(nullptr);
        SysLocksTable.SetAccessLog(nullptr);
    }
};

} // namespace NDataShard
} // namespace NKikimr
