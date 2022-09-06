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
        CheckVersion = TRowVersion::Min();
        BreakVersion = TRowVersion::Min();

        SysLocksTable.SetTxUpdater(this);
        SysLocksTable.SetDb(db);
    }

    TSetupSysLocks(ui64 lockTxId, ui32 lockNodeId, const TRowVersion& readVersion,
            TDataShard& self, ILocksDb* db)
        : SysLocksTable(self.SysLocksTable())
    {
        LockTxId = lockTxId;
        LockNodeId = lockNodeId;
        CheckVersion = readVersion;
        BreakVersion = TRowVersion::Min();

        SysLocksTable.SetTxUpdater(this);
        SysLocksTable.SetDb(db);
    }

    TSetupSysLocks(TOperation::TPtr op,
                   TDataShard &self,
                   ILocksDb* db)
        : SysLocksTable(self.SysLocksTable())
    {
        LockTxId = op->LockTxId();
        LockNodeId = op->LockNodeId();

        if (self.IsMvccEnabled()) {
            auto [readVersion, writeVersion] = self.GetReadWriteVersions(op.Get());

            // check whether the current operation is a part of an out-of-order Tx
            bool outOfOrder = false;
            if (auto &activeOps = self.Pipeline.GetActivePlannedOps()) {
                if (auto it = activeOps.begin(); writeVersion != TRowVersion(it->first.Step, it->first.TxId))
                    outOfOrder = true;
            }

            CheckVersion = readVersion;
            BreakVersion = outOfOrder ? writeVersion : TRowVersion::Min();
        }

        SysLocksTable.SetTxUpdater(this);
        if (!op->LocksCache().Locks.empty())
            SysLocksTable.SetCache(&op->LocksCache());
        else
            SysLocksTable.SetAccessLog(&op->LocksAccessLog());
        SysLocksTable.SetDb(db);
    }

    ~TSetupSysLocks() {
        SysLocksTable.SetTxUpdater(nullptr);
        SysLocksTable.SetCache(nullptr);
        SysLocksTable.SetAccessLog(nullptr);
        SysLocksTable.SetDb(nullptr);
    }
};

} // namespace NDataShard
} // namespace NKikimr
