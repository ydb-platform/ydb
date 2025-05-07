#pragma once

#include <openssl/sha.h>

#include <library/cpp/string_utils/base64/base64.h>

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

inline void LogIntegrityTrailsLocks(const TActorContext& ctx, const ui64 tabletId, const ui64 txId, const TVector<ui64>& locks) {
    if (locks.empty()) {
        return;
    }

    auto logFn = [&]() {
        TStringStream ss;

        LogKeyValue("Component", "DataShard", ss);
        LogKeyValue("Type", "Locks", ss);
        LogKeyValue("TabletId", ToString(tabletId), ss);
        LogKeyValue("PhyTxId", ToString(txId), ss);

        ss << "BreakLocks: [";
        for (const auto& lock : locks) {
            ss << lock << " ";
        }
        ss << "]";

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, logFn());

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
        LogKeyValue("Status", statusString, ss);

        return ss.Str();
    };

    LOG_INFO_S(ctx, NKikimrServices::DATA_INTEGRITY, logFn());
}

}
}
