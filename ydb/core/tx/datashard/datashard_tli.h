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
    auto stlogMessage = YDB_LOG_CREATE_MESSAGE(
        {"tabletId", ToString(tabletId)},
        {"message", message});

    // Log to TLI service (only if we have victim query trace IDs)
    if (canLogTli) {
        auto stlogMessageTLI = stlogMessage;
        YDB_LOG_UPDATE_MESSAGE(stlogMessageTLI,
            {"component", "DataShard"});

        if (breakerQuerySpanId && *breakerQuerySpanId != 0) {
            YDB_LOG_UPDATE_MESSAGE(stlogMessageTLI,
                {"breakerQuerySpanId", ToString(*breakerQuerySpanId)});
        }
        TStringStream victimQuerySpanIdStr;
        for (size_t i = 0; i < victimQuerySpanIds.size(); ++i) {
            victimQuerySpanIdStr << victimQuerySpanIds[i];
            if (i + 1 < victimQuerySpanIds.size()) {
                victimQuerySpanIdStr << " ";
            }
        }
        YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::TLI, "",
            stlogMessageTLI,
            {"victimQuerySpanIds", victimQuerySpanIdStr.Str()});
    }

    // Log to DATA_INTEGRITY service (only if we have broken locks)
    if (canLogIntegrity) {
        TStringStream brokenLocksStr;
        for (size_t i = 0; i < brokenLocks.size(); ++i) {
            brokenLocksStr << brokenLocks[i];
            if (i + 1 < brokenLocks.size()) {
                brokenLocksStr << " ";
            }
        }

        YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
            stlogMessage,
            {"component", "DataShard"},
            {"type", "Locks"},
            {"brokenLocks", brokenLocksStr.Str()});
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
    TStructuredMessage stlogMessage = YDB_LOG_CREATE_MESSAGE(
        {"tabletId", ToString(tabletId)},
        {"Message", message});

    if (victimQuerySpanId && *victimQuerySpanId != 0) {
        YDB_LOG_UPDATE_MESSAGE(stlogMessage,
            {"VictimQuerySpanId", ToString(*victimQuerySpanId)});
    }
    if (currentQuerySpanId && *currentQuerySpanId != 0) {
        YDB_LOG_UPDATE_MESSAGE(stlogMessage,
            {"CurrentQuerySpanId", ToString(*currentQuerySpanId)});
    }

    // Log to TLI service
    if (tliEnabled) {
        YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
            stlogMessage,
            {"component", "DataShard"});
    }

    // Log to DATA_INTEGRITY service
    if (integrityEnabled) {
        YDB_LOG_TRACE_CTX_COMP(ctx, NKikimrServices::DATA_INTEGRITY, "",
            stlogMessage,
            {"component", "DataShard"},
            {"type", "Locks"});
    }
}

}
}
