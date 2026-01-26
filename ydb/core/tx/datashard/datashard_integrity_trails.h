#pragma once

#include <openssl/sha.h>

#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/data_integrity_trails/data_integrity_trails.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/locks/sys_tables.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>

namespace NKikimr {

namespace NDataIntegrity {

inline void WriteTablePoint(const TConstArrayRef<NKikimr::TCell>& point, TStringStream& output) {
    std::string result;
    result.resize(SHA256_DIGEST_LENGTH);

    SHA256_CTX sha256;
    if (!SHA256_Init(&sha256)) {
        return;
    }

    for (size_t i = 0; i < point.size(); ++i) {
        const NKikimr::TCell& cell = point[i];
        if (!SHA256_Update(&sha256, cell.Data(), cell.Size())) {
            return;
        }
    }

    if (!SHA256_Final(reinterpret_cast<unsigned char*>(&result[0]), &sha256)) {
        return;
    }

    output << Base64Encode(result);
}

inline void WriteTableRange(const NKikimr::TTableRange& range, const TVector<NScheme::TTypeInfo>& types, TStringStream& output) {
    const auto keysLogMode = AppData()->DataIntegrityTrailsConfig.HasKeysLogMode()
        ? AppData()->DataIntegrityTrailsConfig.GetKeysLogMode()
        : NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_HASHED;

    if (range.Point) {
        if (keysLogMode == NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_ORIGINAL) {
            output << DebugPrintPoint(types, range.From, *AppData()->TypeRegistry);
        } else {
            WriteTablePoint(range.From, output);
        }
    } else {
        if (keysLogMode == NKikimrProto::TDataIntegrityTrailsConfig_ELogMode_ORIGINAL) {
            output << DebugPrintRange(types, range, *AppData()->TypeRegistry);
        } else {
            output << (range.InclusiveFrom ? "[" : "(");
            WriteTablePoint(range.From, output);
            output << " ; ";
            WriteTablePoint(range.To, output);
            output << (range.InclusiveTo ? "]" : ")");
        }
    }
}

inline void LogIntegrityTrailsKeys(const NActors::TActorContext& ctx, const ui64 tabletId, const ui64 txId, const NMiniKQL::IEngineFlat::TValidationInfo& keys) {
    if (IS_DEBUG_LOG_ENABLED(NKikimrServices::DATA_INTEGRITY)) {
        if (keys.HasWrites()) {
            const int batchSize = 10;
            bool first = true;
            for (size_t offset = 0; offset < keys.Keys.size(); offset += batchSize) {
                TStringStream ss;

                LogKeyValue("Component", "DataShard", ss);
                LogKeyValue("Type", "Keys", ss);
                LogKeyValue("TabletId", ToString(tabletId), ss);
                LogKeyValue("PhyTxId", ToString(txId), ss);

                for (size_t i = offset, j = 0; i < keys.Keys.size() && j < batchSize; i++, j++) {
                    auto& keyDef = keys.Keys[i].Key;

                    if (TSysTables::IsSystemTable(keyDef->TableId)) {
                        continue;
                    }

                    if (first) {
                        LogKeyValue("TableId", ToString(keyDef->TableId), ss);
                        first = false;
                    }

                    auto& range = keyDef->Range;
                    TString rowOp;
                    switch (keyDef->RowOperation) {
                        case NKikimr::TKeyDesc::ERowOperation::Unknown:
                            rowOp = "Unknown";
                            break;
                        case NKikimr::TKeyDesc::ERowOperation::Read:
                            rowOp = "Read";
                            break;
                        case NKikimr::TKeyDesc::ERowOperation::Update:
                            rowOp = "Update";
                            break;
                        case NKikimr::TKeyDesc::ERowOperation::Erase:
                            rowOp = "Erase";
                            break;
                        default:
                            rowOp = "Invalid operation";
                            break;
                    }

                    LogKeyValue("Op", rowOp, ss);

                    ss << "Key: ";
                    WriteTableRange(range, keyDef->KeyColumnTypes, ss);

                    if (i + 1 < keys.Keys.size() && j + 1 < batchSize) {
                        ss << ",";
                    }
                }

                LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, ss.Str());
            }
        }
    }
}

// Unified function that logs lock breaking events to both integrity trails and TLI systems
inline void LogLocksBroken(const NActors::TActorContext& ctx, const ui64 tabletId, TStringBuf message,
                           const TVector<ui64>& brokenLocks, TMaybe<ui64> breakerQueryTraceId = Nothing(),
                           const TVector<ui64>& victimQueryTraceIds = {}) {
    // Check if logging is enabled before formatting (performance optimization)
    const bool tliEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::TLI);
    const bool integrityEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::DATA_INTEGRITY);
    if (!tliEnabled && !integrityEnabled) {
        return;
    }

    // Determine what we can actually log for each service
    const bool canLogTli = tliEnabled && !victimQueryTraceIds.empty();
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
        if (breakerQueryTraceId && *breakerQueryTraceId != 0) {
            LogKeyValue("QueryTraceId", ToString(*breakerQueryTraceId), ss);
            LogKeyValue("BreakerQueryTraceId", ToString(*breakerQueryTraceId), ss);
        }
        ss << "VictimQueryTraceIds: [";
        for (size_t i = 0; i < victimQueryTraceIds.size(); ++i) {
            ss << victimQueryTraceIds[i];
            if (i + 1 < victimQueryTraceIds.size()) {
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
                              TMaybe<ui64> victimQueryTraceId = Nothing(),
                              TMaybe<ui64> currentQueryTraceId = Nothing()) {
    // Check if logging is enabled before formatting (performance optimization)
    const bool tliEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::TLI);
    const bool integrityEnabled = IS_INFO_LOG_ENABLED(NKikimrServices::DATA_INTEGRITY);
    if (!tliEnabled && !integrityEnabled) {
        return;
    }

    // Build message body once (everything except Component and Type)
    TStringStream bodySs;
    LogKeyValue("TabletId", ToString(tabletId), bodySs);
    if (victimQueryTraceId && *victimQueryTraceId != 0) {
        // QueryTraceId = VictimQueryTraceId for victim logs
        LogKeyValue("QueryTraceId", ToString(*victimQueryTraceId), bodySs);
        LogKeyValue("VictimQueryTraceId", ToString(*victimQueryTraceId), bodySs);
    }
    if (currentQueryTraceId && *currentQueryTraceId != 0) {
        LogKeyValue("CurrentQueryTraceId", ToString(*currentQueryTraceId), bodySs);
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

template <typename TxResult>
inline void LogIntegrityTrailsFinish(const NActors::TActorContext& ctx, const ui64 tabletId, const ui64 txId, const typename TxResult::EStatus status) {
    auto logFn = [&]() {
        TString statusString = TxResult::EStatus_descriptor()->FindValueByNumber(status)->name();

        TStringStream ss;

        LogKeyValue("Component", "DataShard", ss);
        LogKeyValue("Type", "Finished", ss);
        LogKeyValue("TabletId", ToString(tabletId), ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);
        LogKeyValue("Status", statusString, ss, true);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, logFn());
}

}
}
