#pragma once

#include "defs.h"
#include "cms_state.h"
#include "scheme.h"

#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <util/datetime/base.h>
#include <util/generic/set.h>

namespace NKikimr::NCms {

using NTabletFlatExecutor::TTransactionContext;

/**
 * Class for log manipulation in local CMS DB.
 */
class TLogger {
public:
    TLogger(TCmsStatePtr state);

    TString GetLogMessage(const NKikimrCms::TLogRecord &rec, NKikimrCms::ETextFormat format) const;

    bool DbCleanupLog(TTransactionContext& txc, const TActorContext& ctx);
    bool DbLoadLogTail(const NKikimrCms::TLogFilter &filter, TVector<NKikimrCms::TLogRecord> &result, TTransactionContext& txc);
    void DbLogData(const NKikimrCms::TLogRecordData &data, TTransactionContext& txc, const TActorContext& ctx);

private:
    TCmsStatePtr State;
};

} // namespace NKikimr::NCms
