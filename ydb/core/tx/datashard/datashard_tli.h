#pragma once

#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/log.h>
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
    TStringStream bodySs;
    LogKeyValue("TabletId", ToString(tabletId), bodySs);
    LogKeyValue("Message", message, bodySs, true);
    TString messageBody = bodySs.Str();

    // Log to TLI service (only if we have victim query trace IDs)
    if (canLogTli) {
        TStringStream ss;
        LogKeyValue("Component", "DataShard", ss);
        if (breakerQuerySpanId && *breakerQuerySpanId != 0) {
            LogKeyValue("BreakerQuerySpanId", ToString(*breakerQuerySpanId), ss);
        }
        ss << "VictimQuerySpanIds: [";
        for (size_t i = 0; i < victimQuerySpanIds.size(); ++i) {
            ss << victimQuerySpanIds[i];
            if (i + 1 < victimQuerySpanIds.size()) {
                ss << " ";
            }
        }
        ss << "], ";

        ss << messageBody;
        LOG_INFO_S(ctx, NKikimrServices::TLI, ss.Str());
    }

    // Log to DATA_INTEGRITY service (only if we have broken locks)
    if (canLogIntegrity) {
        TStringStream ss;
        LogKeyValue("Component", "DataShard", ss);
        LogKeyValue("Type", "Locks", ss);
        ss << "BrokenLocks: [";
        for (size_t i = 0; i < brokenLocks.size(); ++i) {
            ss << brokenLocks[i];
            if (i + 1 < brokenLocks.size()) {
                ss << " ";
            }
        }
        ss << "], ";
        ss << messageBody;
        LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, ss.Str());
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
    TStringStream bodySs;
    LogKeyValue("TabletId", ToString(tabletId), bodySs);
    if (victimQuerySpanId && *victimQuerySpanId != 0) {
        LogKeyValue("VictimQuerySpanId", ToString(*victimQuerySpanId), bodySs);
    }
    if (currentQuerySpanId && *currentQuerySpanId != 0) {
        LogKeyValue("CurrentQuerySpanId", ToString(*currentQuerySpanId), bodySs);
    }
    LogKeyValue("Message", message, bodySs, /*last*/ true);
    TString messageBody = bodySs.Str();

    // Log to TLI service
    if (tliEnabled) {
        TStringStream ss;
        LogKeyValue("Component", "DataShard", ss);
        ss << messageBody;
        LOG_INFO_S(ctx, NKikimrServices::TLI, ss.Str());
    }

    // Log to DATA_INTEGRITY service
    if (integrityEnabled) {
        TStringStream ss;
        LogKeyValue("Component", "DataShard", ss);
        LogKeyValue("Type", "Locks", ss);
        ss << messageBody;
        LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, ss.Str());
    }
}

}
}
