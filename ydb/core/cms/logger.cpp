#include "logger.h"
#include "log_formatter.h"

#include <util/generic/utility.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS

namespace NKikimr::NCms {

using namespace NKikimrCms;

TLogger::TLogger(TCmsStatePtr state)
    : State(state)
{
}

TString TLogger::GetLogMessage(const NKikimrCms::TLogRecord &rec, NKikimrCms::ETextFormat format) const {
    switch (format) {
    case TEXT_FORMAT_NONE:
        return "";
    case TEXT_FORMAT_SHORT:
        return TLogFormatter<TEXT_FORMAT_SHORT>::Format(rec);
    case TEXT_FORMAT_DETAILED:
        return TLogFormatter<TEXT_FORMAT_DETAILED>::Format(rec);
    default:
        return TStringBuilder() << "[unsupported format]" << format;
    }
}

bool TLogger::DbCleanupLog(TTransactionContext &txc, const TActorContext &ctx) {
    NIceDb::TNiceDb db(txc.DB);
    TInstant fromDate = ctx.Now() - State->Config.LogConfig.TTL;
    ui64 from = Max<ui64>() - fromDate.GetValue();

    YDB_LOG_CTX_DEBUG(ctx, "Cleanup log records until",
        {"fromDate", fromDate});

    auto rowset = db.Table<Schema::LogRecords>().GreaterOrEqual(from)
        .Select<Schema::LogRecords::Timestamp>();

    if (!rowset.IsReady())
        return false;

    TVector<ui64> ids;
    while (!rowset.EndOfSet()) {
        ids.push_back(rowset.GetValue<Schema::LogRecords::Timestamp>());

        if (!rowset.Next())
            return false;
    }

    YDB_LOG_CTX_DEBUG(ctx, "Removing log records",
        {"size", ids.size()});

    for (auto id : ids)
        db.Table<Schema::LogRecords>().Key(id).Delete();

    return true;
}

bool TLogger::DbLoadLogTail(const NKikimrCms::TLogFilter &filter, TVector<NKikimrCms::TLogRecord> &result, TTransactionContext &txc) {
    result.clear();

    ui64 from = 0;
    ui64 to = Max<ui64>() - filter.GetMinTimestamp();
    ui64 skip = filter.GetOffset();
    ui64 remain = Min<ui32>(filter.GetLimit(), 10000);
    ui32 type = filter.GetRecordType();

    if (filter.GetMaxTimestamp())
        from = Max<ui64>() - filter.GetMaxTimestamp();

    NIceDb::TNiceDb db(txc.DB);
    auto rowset = db.Table<Schema::LogRecords>().GreaterOrEqual(from)
        .Select<Schema::LogRecords::TColumns>();

    if (!rowset.IsReady())
        return false;

    while (remain && !rowset.EndOfSet()) {
        auto timestamp = rowset.GetValue<Schema::LogRecords::Timestamp>();
        if (timestamp > to)
            break;

        auto data = rowset.GetValue<Schema::LogRecords::Data>();
        if (!type || data.GetRecordType() == type) {
            if (skip) {
                --skip;
            } else {
                result.push_back(NKikimrCms::TLogRecord());
                result.back().SetTimestamp(Max<ui64>() - timestamp);
                result.back().SetRecordType(data.GetRecordType());
                result.back().MutableData()->Swap(&data);
                --remain;
            }
        }

        if (remain && !rowset.Next())
            return false;
    }

    return true;
}

void TLogger::DbLogData(const TLogRecordData &data, TTransactionContext &txc, const TActorContext &ctx) {
    if (!State->Config.IsLogEnabled(data.GetRecordType()))
        return;

    ui64 timestamp = ctx.Now().GetValue();

    if (timestamp <= State->LastLogRecordTimestamp)
        timestamp = State->LastLogRecordTimestamp + 1;
    State->LastLogRecordTimestamp = timestamp;

    YDB_LOG_CTX_TRACE(ctx, "Add log record to local DB",
        {"timestamp", timestamp},
        {"data", data.ShortDebugString()});

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::LogRecords>().Key(Max<ui64>() - timestamp)
        .Update<Schema::LogRecords::Data>(data);
}

} // namespace NKikimr::NCms
