#pragma once

#include <openssl/sha.h>
#include <sstream>

#include <library/cpp/string_utils/base64/base64.h>

#include <ydb/core/scheme/scheme_tabledefs.h>
#include <ydb/core/engine/mkql_engine_flat.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>
#include <ydb/core/tx/locks/sys_tables.h>

namespace NKikimr {

inline void WritePoint(const TConstArrayRef<NKikimr::TCell>& point, std::stringstream& output) {
    std::string result;
    result.resize(SHA_DIGEST_LENGTH);

    SHA_CTX sha1;
    if (!SHA1_Init(&sha1)) {
        return;
    }

    for (size_t i = 0; i < point.size(); ++i) {
        const NKikimr::TCell& cell = point[i];
        if (!SHA1_Update(&sha1, cell.Data(), cell.Size())) {
            return;
        }
    }

    if (!SHA1_Final(reinterpret_cast<unsigned char*>(&result[0]), &sha1)) {
        return;
    }

    output << Base64Encode(result);
}

inline void WriteTableRange(const NKikimr::TTableRange &range, std::stringstream& output) {
    if (range.Point) {
        WritePoint(range.From, output);
    } else {
        output << (range.InclusiveFrom ? "[" : "(");
        WritePoint(range.From, output);
        output << " ; ";
        WritePoint(range.To, output);
        output << (range.InclusiveTo ? "]" : ")");
    }
}

inline void LogIntegrityTrailsKeys(const NActors::TActorContext& ctx, const ui64 tabletId, const ui64 txId, const NMiniKQL::IEngineFlat::TValidationInfo& keys) {
    if (keys.HasWrites()) {
        const int batchSize = 10;

        auto logKeyOp = [&](const TVector<NMiniKQL::IEngineFlat::TValidatedKey>& keys, size_t offset) -> std::string {
            std::stringstream ss;

            ss << "TabletId# " << tabletId << " TxId# " << txId << " ";

            for (size_t i = offset, j = 0; i < keys.size() && j < batchSize; i++, j++) {
                auto& keyDef = keys[i].Key;

                if (TSysTables::IsSystemTable(keyDef->TableId)) {
                    continue;
                }
                
                if (j > 0) {
                    ss << ", ";
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
                ss << "op# " << rowOp << " key# ";
                WriteTableRange(range, ss);
            }
            
            return ss.str();
        };

        for (size_t i = 0; i < keys.Keys.size(); i += batchSize) {
            LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, logKeyOp(keys.Keys, i));
        }
    }
}

inline void LogIntegrityTrailsFinish(const NActors::TActorContext& ctx, const ui64 tabletId, const ui64 txId, const NKikimrTxDataShard::TEvProposeTransactionResult::EStatus status) {
    LOG_DEBUG_S(ctx, NKikimrServices::DATA_INTEGRITY, "TabletId# " << tabletId << " finished TxId# " << txId << " Status# " << status);
}
}
