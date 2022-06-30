#pragma once
#include "defs.h"
#include "operation.h"
#include "datashard_impl.h"

namespace NKikimr {
namespace NDataShard {

struct TSetupSysLocks {
    TSysLocks &SysLocksTable;

    TSetupSysLocks(TOperation::TPtr op,
                   TDataShard &self)
        : SysLocksTable(self.SysLocksTable())
    {
        TLocksUpdate &update = op->LocksUpdate();

        update.Clear();
        update.LockTxId = op->LockTxId();
        update.LockNodeId = op->LockNodeId();

        if (self.IsMvccEnabled()) {
            auto [readVersion, writeVersion] = self.GetReadWriteVersions(op.Get());

            // check whether the current operation is a part of an out-of-order Tx
            bool outOfOrder = false;
            if (auto &activeOps = self.Pipeline.GetActivePlannedOps()) {
                if (auto it = activeOps.begin(); writeVersion != TRowVersion(it->first.Step, it->first.TxId))
                    outOfOrder = true;
            }

            update.CheckVersion = readVersion;
            update.BreakVersion = outOfOrder ? writeVersion : TRowVersion::Min();
        }

        SysLocksTable.SetTxUpdater(&update);
        if (!op->LocksCache().Locks.empty())
            SysLocksTable.SetCache(&op->LocksCache());
        else
            SysLocksTable.SetAccessLog(&op->LocksAccessLog());
    }

    ~TSetupSysLocks() {
        SysLocksTable.SetTxUpdater(nullptr);
        SysLocksTable.SetCache(nullptr);
        SysLocksTable.SetAccessLog(nullptr);
    }
};

} // namespace NDataShard
} // namespace NKikimr
