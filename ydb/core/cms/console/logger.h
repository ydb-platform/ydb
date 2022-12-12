#pragma once
#include "defs.h"

#include "console__scheme.h"

#include <ydb/core/protos/console.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>

namespace NKikimr::NConsole {

using NTabletFlatExecutor::TTransactionContext;

/**
 * Class for log manipulation in local Console DB.
 */
class TLogger {
public:
    TLogger();

    bool DbCleanupLog(ui32 remainEntries,
                      TTransactionContext &txc,
                      const TActorContext &ctx);

    bool DbLoadLogTail(const NKikimrConsole::TLogFilter &filter,
                       TVector<NKikimrConsole::TLogRecord> &result,
                       TTransactionContext &txc);

    void DbLogData(const TString &userSID,
                   const NKikimrConsole::TLogRecordData &data,
                   TTransactionContext &txc,
                   const TActorContext &ctx);

    void SetNextLogItemId(ui64 id);
    void SetMinLogItemId(ui64 id);

private:
    ui64 NextLogItemId = 0;
    ui64 MinLogItemId = 0;
};

} // namespace NKikimr::NConsole
