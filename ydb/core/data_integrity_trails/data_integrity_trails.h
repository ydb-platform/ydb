#pragma once

#include <util/stream/str.h>

#include <ydb/library/actors/struct_log/create_message.h>
#include <ydb/library/actors/struct_log/structured_message.h>

#include <ydb/core/protos/data_integrity_trails.pb.h>

using namespace NActors::NStructuredLog;

namespace NKikimr {
namespace NDataIntegrity {

template <class TransactionSettings>
inline TStructuredMessage LogTxSettings(const TransactionSettings& txSettings) {
    TStructuredMessage message;
    switch (txSettings.tx_mode_case()) {
        case TransactionSettings::kSerializableReadWrite:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "SerializableReadWrite"});
            break;
        case TransactionSettings::kOnlineReadOnly:
            YDB_LOG_UPDATE_MESSAGE(message,
                {"txMode", "OnlineReadOnly"},
                {"allowInconsistentReads", txSettings.online_read_only().allow_inconsistent_reads()});
            break;
        case TransactionSettings::kStaleReadOnly:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "StaleReadOnly"});
            break;
        case TransactionSettings::kSnapshotReadOnly:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "SnapshotReadOnly"});
            break;
        case TransactionSettings::kSnapshotReadWrite:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "SnapshotReadWrite"});
            break;
        case TransactionSettings::kReadCommittedReadWrite:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "ReadCommittedReadWrite"});
            break;
        case TransactionSettings::kStrictSerializableReadWrite:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "StrictSerializableReadWrite"});
            break;
        case TransactionSettings::TX_MODE_NOT_SET:
            YDB_LOG_UPDATE_MESSAGE(message, {"txMode", "Undefined"});
            break;
    }
    return message;
}

template <class TxControl>
inline TStructuredMessage LogTxControl(const TxControl& txControl)
{
    TStructuredMessage message;
    switch (txControl.tx_selector_case()) {
        case TxControl::kTxId:
            YDB_LOG_UPDATE_MESSAGE(message, {"txId", txControl.tx_id()});
            break;
        case TxControl::kBeginTx:
            YDB_LOG_UPDATE_MESSAGE(message,
                {"beginTx", true},
                LogTxSettings(txControl.begin_tx()));
            break;
        case TxControl::TX_SELECTOR_NOT_SET:
            break;
    }

    YDB_LOG_UPDATE_MESSAGE(message, {"needCommitTx", txControl.commit_tx()});
    return message;
}

}
}
