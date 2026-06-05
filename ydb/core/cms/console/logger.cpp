#include "logger.h"
#include "console_impl.h"

#include <util/generic/utility.h>

namespace NKikimr::NConsole {

using namespace NKikimrConsole;

TLogger::TLogger() {}

bool TLogger::DbCleanupLog(ui32 remainEntries,
                           TTransactionContext &txc,
                           const TActorContext &ctx)
{
    if (remainEntries > NextLogItemId) {
        return true;
    }

    ui64 fromId = NextLogItemId - remainEntries;

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Cleanup log records until " << fromId);

    NIceDb::TNiceDb db(txc.DB);

    LOG_DEBUG_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Removing " << (fromId - MinLogItemId + 1) << " log records");

    for (ui64 id = MinLogItemId; id <= fromId; ++id)
        db.Table<Schema::LogRecords>().Key(id).Delete();

    MinLogItemId = fromId + 1;

    db.Table<Schema::Config>().Key(TConsole::ConfigKeyMinLogItemId)
        .Update<Schema::Config::Value>(ToString(MinLogItemId));

    return true;
}

bool TLogger::DbLoadLogTail(const NKikimrConsole::TLogFilter &filter,
                            TVector<NKikimrConsole::TLogRecord> &result,
                            TTransactionContext &txc)
{
    result.clear();

    ui64 remain = Min<ui32>(filter.GetLimit(), 10000);

    ui64 timestamp = Max<ui64>();

    if (filter.HasReverse() && filter.GetReverse()) {
        timestamp = Min<ui64>();
    }

    if (filter.HasFromTimestamp()) {
        timestamp = filter.GetFromTimestamp();
    }

    bool reverse = filter.HasReverse() && filter.GetReverse();

    auto checkTimestamp = [&](ui64 ts) {
        if (reverse) {
            return ts >= timestamp;
        } else {
            return ts <= timestamp;
        }
    };

    THashSet<TString> users;

    for (size_t i = 0; i < filter.UsersSize(); i++) {
        users.insert(filter.GetUsers(i));
    }

    THashSet<TString> excludeUsers;

    for (size_t i = 0; i < filter.ExcludeUsersSize(); i++) {
        excludeUsers.insert(filter.GetExcludeUsers(i));
    }

    auto checkUser = [&](TString& user) {
        if (!users.empty()) {
            return users.contains(user);
        } else if (!excludeUsers.empty()) {
            return !excludeUsers.contains(user);
        }

        return true;
    };

    THashSet<ui32> affectedKinds;

    for (size_t i = 0; i < filter.AffectedKindsSize(); i++) {
        affectedKinds.insert(filter.GetAffectedKinds(i));
    }

    auto checkAffected = [&](const NKikimrConsole::TLogRecordData& data) {
        if (affectedKinds.size() != 0) {
            for (size_t i = 0; i < data.AffectedKindsSize(); i++) {
                if(affectedKinds.contains(data.GetAffectedKinds(i))) {
                    return true;
                }
            }

            return false;
        }

        return true;
    };

    NIceDb::TNiceDb db(txc.DB);

    auto processRowset = [&](auto&& rowset) {
        if (!rowset.IsReady())
            return false;

        while (remain && !rowset.EndOfSet()) {
            auto ts = rowset.template GetValue<Schema::LogRecords::Timestamp>();
            auto id = rowset.template GetValue<Schema::LogRecords::Id>();
            auto user = rowset.template GetValue<Schema::LogRecords::UserSID>();
            NKikimrConsole::TLogRecordData data;
            Y_PROTOBUF_SUPPRESS_NODISCARD data.ParseFromString(rowset.template GetValue<Schema::LogRecords::Data>());

            if (checkTimestamp(ts) && checkUser(user) && checkAffected(data)) {
                result.push_back(NKikimrConsole::TLogRecord());
                result.back().SetId(id);
                result.back().SetTimestamp(ts);
                result.back().SetUser(user);
                result.back().MutableData()->Swap(&data);
                --remain;
            }

            if (remain && !rowset.Next()) {
                return false;
            }
        }

        return true;
    };

    auto table = db.Table<Schema::LogRecords>();

    if (reverse && filter.HasFromId()) {
        return processRowset(table
            .GreaterOrEqual(filter.GetFromId())
            .Select<Schema::LogRecords::TColumns>());
    } else if (reverse && !filter.HasFromId()) {
        return processRowset(table
            .Select<Schema::LogRecords::TColumns>());
    } else if (!reverse && filter.HasFromId()) {
        return processRowset(table
            .Reverse()
            .LessOrEqual(filter.GetFromId())
            .Select<Schema::LogRecords::TColumns>());
    } else {
        return processRowset(table
            .Reverse()
            .Select<Schema::LogRecords::TColumns>());
    }
 }

void TLogger::DbLogData(const TString &userSID,
                        const TLogRecordData &data,
                        TTransactionContext &txc,
                        const TActorContext &ctx)
{
    ui64 timestamp = ctx.Now().GetValue();

    TString serializedData = data.SerializeAsString();

    LOG_TRACE_S(ctx, NKikimrServices::CMS_CONFIGS,
                "Add log record to local DB"
                << " timestamp=" << timestamp
                << " data=" << data.ShortDebugString());

    NIceDb::TNiceDb db(txc.DB);
    db.Table<Schema::LogRecords>().Key(NextLogItemId)
        .Update<Schema::LogRecords::Timestamp>(timestamp)
        .Update<Schema::LogRecords::UserSID>(userSID)
        .Update<Schema::LogRecords::Data>(serializedData);

    db.Table<Schema::Config>().Key(TConsole::ConfigKeyNextLogItemId)
        .Update<Schema::Config::Value>(ToString(++NextLogItemId));
}

void TLogger::SetNextLogItemId(ui64 id) {
    NextLogItemId = id;
}

void TLogger::SetMinLogItemId(ui64 id) {
    MinLogItemId = id;
}

} // namespace NKikimr::NConsole
