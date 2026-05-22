#pragma once

#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/struct_log/create_message.h>
#include <ydb/library/actors/struct_log/structured_message.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {

namespace NDataIntegrity {

// Unified function that logs lock breaking events to both integrity trails and TLI systems
inline void LogLocksBroken(const NActors::TActorContext& ctx, const ui64 tabletId, TStringBuf message,
                           const TVector<ui64>& brokenLocks, TMaybe<ui64> breakerQuerySpanId = Nothing(),
                           const TVector<ui64>& victimQuerySpanIds = {}) {
    // Check if logging is enabled before formatting (performance optimization)
    const bool tliEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::TLI);
    const bool integrityEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::DATA_INTEGRITY);
    if (!tliEnabled && !integrityEnabled) {
        return;
    }

    // Determine what we can actually log for each service
    const bool canLogTli = tliEnabled && !victimQuerySpanIds.empty();
    const bool canLogIntegrity = integrityEnabled && !brokenLocks.empty();

    // Early return if neither service has anything to log
    if (!canLogTli && !canLogIntegrity) {
        return;
    }

    // Build message body once (everything except Component and Type)
    NActors::NStructuredLog::TStructuredMessage bodySs;
    LogKeyValue("TabletId", ToString(tabletId), bodySs);
    LogKeyValue("Message", message, bodySs);

    // Log to TLI service (only if we have victim query trace IDs)
    if (canLogTli) {
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "DataShard", ss);
        if (breakerQuerySpanId && *breakerQuerySpanId != 0) {
            LogKeyValue("BreakerQuerySpanId", ToString(*breakerQuerySpanId), ss);
        }

        TStringStream victimQuerySpanIdsStr;
        for (size_t i = 0; i < victimQuerySpanIds.size(); ++i) {
            victimQuerySpanIdsStr << victimQuerySpanIds[i];
            if (i + 1 < victimQuerySpanIds.size()) {
                victimQuerySpanIdsStr << " ";
            }
        }
        ss.AppendValue({"VictimQuerySpanIds"}, victimQuerySpanIdsStr.Str());
        ss.AppendSubMessage({"MessageBody"}, bodySs);

        YDB_LOG_CTX_COMP_TRACE(ctx, TLI, "Transaction locks broken", ss);
    }

    // Log to DATA_INTEGRITY service (only if we have broken locks)
    if (canLogIntegrity) {
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "DataShard", ss);
        LogKeyValue("Type", "Locks", ss);

        TStringStream brokenLocksStr;
        for (size_t i = 0; i < brokenLocks.size(); ++i) {
            brokenLocksStr << brokenLocks[i];
            if (i + 1 < brokenLocks.size()) {
                brokenLocksStr << " ";
            }
        }
        ss.AppendValue({"BrokenLocks"}, brokenLocksStr.Str());

        ss.AppendSubMessage({"MessageBody"}, bodySs);

        YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Transaction locks broken", ss);
    }

}

// Log victim detection in DataShard (when a transaction detects its locks were broken)
inline void LogVictimDetected(const NActors::TActorContext& ctx, const ui64 tabletId, TStringBuf message,
                              TMaybe<ui64> victimQuerySpanId = Nothing(),
                              TMaybe<ui64> currentQuerySpanId = Nothing()) {
    // Check if logging is enabled before formatting (performance optimization)
    const bool tliEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::TLI);
    const bool integrityEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::DATA_INTEGRITY);
    if (!tliEnabled && !integrityEnabled) {
        return;
    }

    // Build message body once (everything except Component and Type)
    NActors::NStructuredLog::TStructuredMessage bodySs;
    LogKeyValue("TabletId", ToString(tabletId), bodySs);
    if (victimQuerySpanId && *victimQuerySpanId != 0) {
        LogKeyValue("VictimQuerySpanId", ToString(*victimQuerySpanId), bodySs);
    }
    if (currentQuerySpanId && *currentQuerySpanId != 0) {
        LogKeyValue("CurrentQuerySpanId", ToString(*currentQuerySpanId), bodySs);
    }
    LogKeyValue("Message", message, bodySs);

    // Log to TLI service
    if (tliEnabled) {
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "DataShard", ss);
        ss.AppendSubMessage({"message_body"}, bodySs);

        YDB_LOG_CTX_COMP_TRACE(ctx, TLI, "Transaction victim detected", ss);
    }

    // Log to DATA_INTEGRITY service
    if (integrityEnabled) {
        NActors::NStructuredLog::TStructuredMessage ss;
        LogKeyValue("Component", "DataShard", ss);
        LogKeyValue("Type", "Locks", ss);

        ss.AppendSubMessage({"message_body"}, bodySs);

        YDB_LOG_CTX_COMP_TRACE(ctx, DATA_INTEGRITY, "Transaction victim detected", ss);
    }
}

}
}
